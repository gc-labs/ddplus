/*
  ddless
  
  Instead of dd if=/dev/source of=/dev/target, we do this once, while reading we
  record segment (16k) signatures. Next time, we read, compare the signature, if
  nothing changes, we continue to read. Otherwise, we update the signature and
  write to the target.
  
  There is a reader and ddless thread which decouples the disk read from the
  signature calculation. With single threaded code we achieved 100MB/s read
  throughput, while the threaded version read at 150MB/s.
  
  The next revision of the code introduces general worker threads that do both
  the reading and signature calculation, simplifying the code (lock-free)
  and increasing the throughput.
  
  Steffen Plotner, 2008, 2009, 2010
*/

#include "ddless.h"
#include "dd_log.h"
#include "dd_murmurhash2.h"
#include "dd_file.h"
#include "dd_map.h"

parms_struct parms;
thread_struct *threads;

//-----------------------------------------------------------------------------
// memory dump
//-----------------------------------------------------------------------------
void mem_dump(char *mem, u_int64_t size)
{
	u_int32_t cols = 32;
	u_int64_t i;
	char *ptr = mem;
	char *line;	

	line = malloc(1024);
	if (!line)
		return;
	*line = '\0';

	for (i=0; i<size; i++)
	{
		if (i % cols == 0 && *line)
		{
			printf("%08llx: %s\n", (long long unsigned)i - cols , line);
			*line = '\0';
		}
		snprintf(line+strlen(line), 10, "%02x ",*ptr);
		ptr++;
		
	}
	printf("%08llx: %s\n", (long long unsigned)i, line);
	free(line);
}

//-----------------------------------------------------------------------------
// initialize worker reader
//-----------------------------------------------------------------------------
int ddless_worker_init_reader(thread_struct *thread)
{
	dd_log(LOG_INFO, "worker_id: %d (%p)", thread->worker_id, thread);
	
	//
	// allocate page size aligned work buffers (required by O_DIRECT)
	//
	dd_log(LOG_INFO, "%u MB read buffer", READ_BUFFER_SIZE / MEGABYTE_FACTOR);
	
	#ifdef SUNOS
	if ((thread->aligned_buffer = memalign(getpagesize(), READ_BUFFER_SIZE)) == NULL )
	#else
	if (posix_memalign((void**)&thread->aligned_buffer, getpagesize(), READ_BUFFER_SIZE))
	#endif
	{
		dd_log(LOG_ERR, "unable to allocate buffers with READ_BUFFER_SIZE=%d",READ_BUFFER_SIZE);
		return -1;
	}
	dd_log(LOG_DEBUG, "buffer aligned address: %p",thread->aligned_buffer);

	//
	// open source device readonly
	//
	if ((thread->source_fd = dd_dev_open_ro(parms.source_dev, parms.o_direct)) == -1 )
	{
		dd_log(LOG_ERR, "unable to open source device: %s",parms.source_dev);
		return -1;
	}
	dd_log(LOG_INFO, "opened source device: %s",parms.source_dev);
	
	return 0;
}

//-----------------------------------------------------------------------------
// initialize worker writer
//-----------------------------------------------------------------------------
int ddless_worker_init_writer(thread_struct *thread)
{
	dd_log(LOG_INFO, "worker_id: %d (%p)", thread->worker_id, thread);

	//
	// open the target (o_direct is not sensible for writing)
	//
	if ((thread->target_fd = dd_dev_open_rw(parms.target_dev, 0)) == -1 )
	{
		dd_log(LOG_ERR, "unable to open target device: %s",parms.target_dev);
		return -1;
	}
	dd_log(LOG_INFO, "opened target device: %s",parms.target_dev);
	
	return 0;
}

//-----------------------------------------------------------------------------
// initialize delta writer
//-----------------------------------------------------------------------------
int ddless_worker_init_delta_writer(thread_struct *thread)
{
	dd_log(LOG_INFO, "worker_id: %d (%p)", thread->worker_id, thread);

	//
	// open the target (o_direct is not sensible for writing)
	//
	if ((thread->target_fd = dd_dev_open_rw(parms.delta_file, 0)) == -1 )
	{
		dd_log(LOG_ERR, "unable to open target device: %s",parms.delta_file);
		return -1;
	}
	dd_log(LOG_INFO, "opened target device: %s",parms.delta_file);
	
	return 0;
}

//-----------------------------------------------------------------------------
// process buffer
//-----------------------------------------------------------------------------
int process_buffer(thread_struct *thread,
	u_int64_t source_pos,
	size_t read_size)
{
	int last_worker = ( thread->worker_id == parms.workers - 1);
	
	dd_log(LOG_DEBUG, "process buffer source_pos: %lu read_size: %d", 
		source_pos, read_size);

	//
	// prepare the checksum pointer
	//
	checksum_struct *checksum_ptr = NULL;
	if ( parms.checksum_array != NULL )
	{
		checksum_ptr = parms.checksum_array + (source_pos / SEGMENT_SIZE);
		dd_log(LOG_DEBUG,"checksum_array=%p checksum_ptr=%p offset=%u",
			parms.checksum_array, checksum_ptr, (source_pos / SEGMENT_SIZE));
	}

	//
	// position the source file pointer
	//
	if ( lseek64(thread->source_fd, source_pos, SEEK_SET) == -1 )
	{
		dd_log(LOG_ERR,"seek set to read offset: %llu failed",source_pos);
		return -1;
	}

	//
	// read data
	//
	int buffer_read_bytes = 0;
	if ( (buffer_read_bytes = read(thread->source_fd, 
		thread->aligned_buffer, read_size)) == -1 )
	{
		dd_log(LOG_ERR, "unable to read from source device");
		return -1;
	}

	//
	// EOF (on SunOS we do not get here if rdsk is used!)
	//
	if ( buffer_read_bytes == 0 )
	{
		dd_log(LOG_DEBUG,"buffer_read_bytes is 0, done");
		return buffer_read_bytes;
	}

	//
	// check for short reads, this should only happen to the last worker
	//
	if ( !last_worker && buffer_read_bytes != read_size)
	{
		dd_log(LOG_ERR, "expected %d bytes, only got %d bytes, not the last worker!",
			read_size, buffer_read_bytes);
		return -1;
	}

	//
	// now that we have accepted the buffer's data, update stats
	//
	thread->stats_read_buffers++;

	//
	// is ddzone mode we just read and we are done
	//
	if ( parms.runmode == RUNMODE_DDZONE )
	{
		return buffer_read_bytes;
	}
	
	//
	// setup of the aligned buffer pointer
	//
	void *buf;
	buf = thread->aligned_buffer;
	memset(thread->seg_bytes_dirty_map, 0, sizeof(int) * (BUFFER_SEGMENTS+1));

	//
	// process using SEGMENT_SIZE chunks of data, the last
	// segment might not be the full size
	//
	u_int32_t buf_offset = 0;	// buffer offset, incremented usually by SEGMENT_SIZE
	u_int32_t seg_bytes = 0;	// how many bytes we can process per segment
	int segment = 0;
	while ( buf_offset < buffer_read_bytes )
	{
		//
		// handle a read that is less than SEGMENT_SIZE
		//
		seg_bytes = SEGMENT_SIZE;
		if ( buf_offset + SEGMENT_SIZE > buffer_read_bytes )
		{
			seg_bytes = buffer_read_bytes - buf_offset;
			dd_log(LOG_INFO,"processing short segment %d bytes", seg_bytes);
		}

		//
		// decide write action based on check sum file availability
		//
		if ( checksum_ptr == NULL )
		{
			//
			// no checksum, then consider the segment dirty to force the write
			//
			thread->seg_bytes_dirty_map[segment] = seg_bytes;

			//
			// record stats
			//
			thread->stats_changed_segments++;
			thread->stats_written_bytes += seg_bytes;
		}
		else
		{
			//
			// compute checksum
			//
			u_int32_t checksum1_murmur;
			u_int32_t checksum2_crc32 = crc32( 0L, Z_NULL, 0 );

			checksum1_murmur = MurmurHash2(buf + buf_offset, seg_bytes, 0xbabeaffe);
			checksum2_crc32 = crc32(checksum2_crc32, buf + buf_offset, seg_bytes);

			//
			// check if we need to write out this segment of data
			// to the output device (i.e. checksums changed) OR
			// if we have a new checksum file
			//
			if ( parms.checksum_file_new || 
					checksum_ptr->checksum1_murmur != checksum1_murmur ||
					checksum_ptr->checksum2_crc32 != checksum2_crc32)
			{
				if ( parms.runmode == RUNMODE_SOURCE_TARGET || parms.runmode == RUNMODE_SOURCE_DELTA)
				{
					//
					// instead of fseek, write the data, we just track the number of
					// bytes to be written within the current segment
					//
					thread->seg_bytes_dirty_map[segment] = seg_bytes;
				}

				dd_log(LOG_DEBUG,"murmur=%08x crc32=%08x -> murmur=%08x crc32=%08x",
					checksum_ptr->checksum1_murmur,
					checksum_ptr->checksum2_crc32,
					checksum1_murmur,
					checksum2_crc32);

				//
				// write out the checksum via mmap'd file
				//
				checksum_ptr->checksum1_murmur = checksum1_murmur;
				checksum_ptr->checksum2_crc32 = checksum2_crc32;

				//
				// record stats
				//
				thread->stats_changed_segments++;
				thread->stats_written_bytes += seg_bytes;
				
				dd_log(LOG_DEBUG, "process buffer source_pos: %llu read_size: %d checksum_ptr: %p", 
						source_pos, read_size, checksum_ptr - parms.checksum_array);
			}

			dd_log(LOG_DEBUG,"checksum_ptr=%p segment=%lu murmur=%08x crc32=%08x",
				checksum_ptr,buf_offset,checksum1_murmur,checksum2_crc32);

			checksum_ptr++;
		}

		buf_offset += SEGMENT_SIZE;
		segment++;
	} // end of process segments of buffer loop

	//
	// write out segments of data, optimising the number of bytes written at once
	//
	int seg_i;			// segment counter i
	int active_segment_bytes = 0;	// number of actively changed segment bytes
	void *buf_dirty_ptr = NULL;	// pointer into buffer at which we have changed data
	u_int64_t write_offset = 0;	// destination device write position

	for(seg_i = 0; seg_i < BUFFER_SEGMENTS+1; seg_i++)
	{
		dd_log(LOG_DEBUG,"dirty segment bytes[%d]=%d",seg_i,thread->seg_bytes_dirty_map[seg_i]);

		//
		// once we encounter non-zero entry we track it and following ones
		//
		if ( thread->seg_bytes_dirty_map[seg_i] != 0 && !active_segment_bytes )
		{
			active_segment_bytes = thread->seg_bytes_dirty_map[seg_i];
			buf_dirty_ptr = buf + (seg_i * SEGMENT_SIZE);
			write_offset = 
				source_pos + 
				(seg_i * SEGMENT_SIZE);
		}
		//
		// once we encounter a non-zero entry and we are tracking, add the bytes to the
		// active segment byte counter
		//
		else if ( thread->seg_bytes_dirty_map[seg_i] != 0 && active_segment_bytes)
		{
			active_segment_bytes += thread->seg_bytes_dirty_map[seg_i];
		}
		//
		// once we enounter a 0 and are actively tracking, we write the data and
		// remember that the array has been forced to end with a 0, so a file that
		// ends as the segment ends will make this terminate correctly.
		//
		else if ( thread->seg_bytes_dirty_map[seg_i] == 0 && active_segment_bytes)
		{


			if ( parms.runmode == RUNMODE_SOURCE_DELTA )
			{
                        	void   *delta_buf_ptr            = buf_dirty_ptr;
	                        u_int64_t delta_bytes_write      = active_segment_bytes;
	                        u_int64_t delta_bytes_written    = 0;
	                       	u_int64_t compress_bytes_written = 0;
	                        int delta_ret;
	                        while(1)
	                        {

                                // dd_log(LOG_INFO, "Writing segment %lu", segment_write_offset);

                                if ((delta_ret = write(parms.delta_fd, (void *)&write_offset, sizeof(write_offset)))==-1)
                                {
                                        dd_log(LOG_ERR,"delta: failed to write segment_write_offset");
                                        exit(1);
                                }

				parms.delta_size     += delta_bytes_write;
	                        fprintf(parms.delta_info_fd, "Writing %llu bytes - completion %5.2f%%\n", 
                                  (long long unsigned)delta_bytes_write, 100*(float)write_offset /(float)parms.source_size_bytes);

				if (parms.compressedflag > 0) {
                                       	// dd_log(LOG_INFO,"compress delta: compressing buffer size %d", delta_bytes_write);
					uLongf bound = compressBound(delta_bytes_write);
					
					if ((delta_ret = compress2 ((Bytef *)parms.zipbuffer, &bound, delta_buf_ptr, delta_bytes_write, parms.ziplevel)) != Z_OK) 
					{
                                        	dd_log(LOG_ERR,"compress delta: failed to compress buffer - %d", delta_ret);
                                        	exit(1);

					}
					dd_log(LOG_INFO, "compressed %ld bytes into %ld - rate %.2f", delta_bytes_write, bound, 100 * (float)bound/(float)delta_bytes_write);
			
					parms.delta_zip_size += bound;

					// delta_bytes_written = delta_bytes_write;
	                        	u_int64_t compress_bytes_write = bound;
					
                                	if ((delta_ret = write(parms.delta_fd, (void *)&compress_bytes_write, sizeof(compress_bytes_write)))==-1)
                                	{
                                        	dd_log(LOG_ERR,"compress delta: failed to write buffer size (measured in BYTES units)");
                                        	exit(1);
                                	}
                                	if ((compress_bytes_written = write(parms.delta_fd, parms.zipbuffer, compress_bytes_write))==-1)
                                	{
                                        	dd_log(LOG_ERR,"compress delta: buffer write failed");
                                        	exit(1);
                                	}
                                	if ( compress_bytes_written-compress_bytes_write == 0 ) {
                                        	parms.delta_writes++;
                                        	break;
                                	}
                                	else {
                                        	dd_log(LOG_ERR,"compress delta: encountered a short write on delta");
                                        	exit(1);
                                	}



				}
				else {
                                	if ((delta_ret = write(parms.delta_fd, (void *)&delta_bytes_write, sizeof(delta_bytes_write)))==-1)
                                	{
                                        	dd_log(LOG_ERR,"delta: failed to write buffer_write_size (measured in SEGMENT_SIZE units)");
                                        	exit(1);
                                	}

                                	if ((delta_bytes_written = write(parms.delta_fd, delta_buf_ptr, delta_bytes_write))==-1)
                                	{
                                        	dd_log(LOG_ERR,"delta write failed");
                                        	exit(1);
                                	}

                                	dd_log(LOG_DEBUG,"seg_i=%d buf_dirty_ptr=%p active_segment_bytes=%d buf_ptr=%p bytes_write:%d bytes_written:%d more_to_write:%d",
                                        	seg_i, buf_dirty_ptr,active_segment_bytes,delta_buf_ptr,delta_bytes_write,delta_bytes_written,delta_bytes_written-delta_bytes_write);

                                	if ( delta_bytes_written-delta_bytes_write == 0 ) {
                                        	parms.delta_writes++;
                                        	break;
                                	}
                                	else {
                                        	dd_log(LOG_ERR,"encountered a short write on delta");
                                        	exit(1);
                                	}
                                }

			    }
			    active_segment_bytes = 0;
			    fflush(parms.delta_info_fd);
			}

			if ( parms.runmode == RUNMODE_SOURCE_TARGET )
			{


			dd_log(LOG_DEBUG,"write_offset %llu, write %u bytes", 
				write_offset, active_segment_bytes);

			//
			// position the target file pointer to write_offset
			//
			if ( lseek64(thread->target_fd, write_offset, SEEK_SET) == -1 )
			{
				dd_log(LOG_ERR,"seek set to write offset: %llu failed",write_offset);
				exit(1);
			}

			//
			// now do the actual write
			//
			void *buf_ptr = buf_dirty_ptr;
			ssize_t bytes_write = active_segment_bytes;
			ssize_t bytes_written = 0;
			while(1)
			{
				if ((bytes_written = write(thread->target_fd, buf_ptr, bytes_write))==-1)
				{
					dd_log(LOG_ERR,"write failed");
					exit(1);
				}

				dd_log(LOG_DEBUG,"seg_i=%d buf_dirty_ptr=%p active_segment_bytes=%d buf_ptr=%p bytes_write:%d bytes_written:%d more_to_write:%d",
					seg_i, buf_dirty_ptr,active_segment_bytes,buf_ptr,bytes_write,bytes_written,bytes_written-bytes_write);

				if ( bytes_written-bytes_write == 0 )
					break;
				else
					dd_log(LOG_INFO,"encountered a short write");

				buf_ptr += bytes_written;
				bytes_write += bytes_written;
			}

			//
			// reset the state machine
			//
			active_segment_bytes = 0;
			} // end of RUNMODE_SOURCE_TARGET
		}

	} // end of SEGMENT process loop (n segments per buffer)

	return buffer_read_bytes;
}

//-----------------------------------------------------------------------------
// worker thread (pthread) for ddmap (read changed segments and writes them)
//-----------------------------------------------------------------------------
/*
 * lun: ddmap algorithm
 *
 * A single 16kb data segment is encoded as a bit, specifically, a bit is set
 * to indicate that a 16kb segment has changed. u32 type words are in use. Each
 * u32 word encodes 512kb of data. u32 words are used in increasing order (i.e.
 * the 1st u32 word refers to the first 512kb of disk data). Within a u32 word,
 * the bits are processed from least to most significant bit (i.e. for the first
 * u32 word, bit 0 is the first 16kb segment of data, bit 1 is the second 16kb
 * segment of data and so on).
 *
 * byte 0 bit 1 maps to 16kb of data (first block of data)
 * byte 0 bit 2 maps to 16kb of data (2nd)
 * byte 0 bit 4 maps to 16kb of data (3rd)
 * byte 0 bit 8 maps to 16kb of data (4th up to bit 32)
 * ...
 */
void *ddmap_worker_thread(thread_struct *thread)
{
	int last_worker = ( thread->worker_id == parms.workers - 1);
	dd_log(LOG_INFO, "worker_id: %d (%p) last_worker: %d", thread->worker_id, thread, last_worker);

	//
	// given the current worker id, determine a suitable location in ddmap to
	// start with - and also calculate the ending position - we don't have
	// a end of file concept here, only a segmentation fault waiting to happen!
	//
	u_int32_t *map = parms.ddmap_data->map;
	u_int32_t *map_start = map +
		((parms.read_buffers_per_worker * READ_BUFFER_SIZE) >> DDMAP_512K_SHIFT) * thread->worker_id;

	u_int32_t *map_end = map +
		((parms.read_buffers_per_worker * READ_BUFFER_SIZE) >> DDMAP_512K_SHIFT) * (thread->worker_id + 1);
	if ( last_worker )
		map_end = map + parms.ddmap_data->map_size;

	dd_log(LOG_INFO, "map: %p...%p map_start: %p map_end: %p", 
		map, map + parms.ddmap_data->map_size, map_start, map_end);
		
	//
	// determine begin seek position given the thread identifier
	//
	size_t read_size = 0;
	u_int64_t source_pos = (parms.read_buffers_per_worker * READ_BUFFER_SIZE) * thread->worker_id;
	u_int64_t source_pos_start = 0;
	dd_log(LOG_INFO,"ddmap initial seek position: %llu", source_pos);

	//
	// process map on a u32 word by word
	//
	u_int32_t map_mask;
	u_int32_t bit;
	u_int32_t bit_prev = 0;
	for (map=map_start; map<map_end; map++)
	{

		//
		// process map on a bit by bit basis
		//
		map_mask = 0x00000001;
		while (map_mask)
		{
			bit = ((*map & map_mask) > 0);
			
			dd_log(LOG_DEBUG2, "map: %p (%08x) mask: %08x source_pos_start: %llu bit: %u bit_prev: %u",
				map, *map, map_mask, source_pos_start, bit, bit_prev);

			if ( bit == 0 && bit_prev == 0 )	// 0 -> 0 (ignore)
			{
			}
			else if ( bit == 1 && bit_prev == 0 )	// 0 -> 1 (start of data)
			{
				source_pos_start = source_pos;
				read_size = SEGMENT_SIZE;
			}
			else if ( bit == 1 && bit_prev == 1 )	// 1 -> 1 (more data, check for max buffer)
			{
				if ( read_size + SEGMENT_SIZE > READ_BUFFER_SIZE )
				{
					dd_log(LOG_DEBUG,"process_buffer > READ_BUFFERS_SIZE");
					dd_log(LOG_DEBUG, "process buffer source_pos: %llu read_size: %d", 
						source_pos_start, read_size);
					if ( process_buffer(thread, source_pos_start, read_size) < 0)
					{
						dd_log(LOG_ERR, "unable to read from source device");
						thread->worker_thread_ccode = -1;
						pthread_exit(NULL);
					}
					
					//
					// reset read size, however, just below it will
					// be initialized for this 1 bit
					//
					source_pos_start = source_pos;
					read_size = 0;
				}
				read_size += SEGMENT_SIZE;
			}
			else if ( bit == 0 && bit_prev == 1 )	// 1 -> 0 (end of data)
			{
				dd_log(LOG_DEBUG,"process_buffer 1 -> 0");
				dd_log(LOG_DEBUG, "process buffer source_pos: %llu read_size: %d", 
					source_pos_start, read_size);
				if ( process_buffer(thread, source_pos_start, read_size) < 0)
				{
					dd_log(LOG_ERR, "unable to read from source device");
					thread->worker_thread_ccode = -1;
					pthread_exit(NULL);
				}
			}

			bit_prev = bit;
			
			//
			// each increment below is with respect to the 16KB segment size
			//
			source_pos += SEGMENT_SIZE;
			map_mask = map_mask << 1;
		}
	}
	
	//
	// if the last bit processed was 1, then write out the rest of the data
	//
	if ( bit_prev == 1 )
	{
		dd_log(LOG_DEBUG,"last bit was 1, flushing...");
		dd_log(LOG_DEBUG, "process buffer source_pos: %llu read_size: %d", 
			source_pos_start, read_size);
		if ( process_buffer(thread, source_pos_start, read_size) < 0)
		{
			dd_log(LOG_ERR, "unable to read from source device");
			thread->worker_thread_ccode = -1;
			pthread_exit(NULL);
		}
	}
	
	pthread_exit(NULL);
}


//-----------------------------------------------------------------------------
// worker thread (pthread) for ddless (reads all and writes changed segments)
//-----------------------------------------------------------------------------
void *ddless_worker_thread(thread_struct *thread)
{
	int last_worker = ( thread->worker_id == parms.workers - 1);
	dd_log(LOG_INFO, "worker_id: %d (%p) last_worker: %d", thread->worker_id, thread, last_worker);

	//
	// ddzone mode/read throttle
	//
	#define DDZONE_DATA_COLUMS 2
	struct timeval buffer_time_start;
	struct timeval buffer_time_end;
	long elapsed_seconds;
	long elapsed_useconds;
	long elapsed_mtime;
	char *zone_tabs = NULL;
	double read_mb_sec;
	int sleep_factor_usec = 0;

	//
	// checksum, pointer is worker dependend
	//
	checksum_struct *checksum_ptr = NULL;
	if ( parms.checksum_array != NULL )
	{
		checksum_ptr = parms.checksum_array +
			((parms.read_buffers_per_worker * READ_BUFFER_SIZE)/SEGMENT_SIZE) * thread->worker_id;
		dd_log(LOG_DEBUG,"checksum_array=%p checksum_ptr=%p",parms.checksum_array,checksum_ptr);
	}
		
	//
	// ddzone worker init
	//
	if ( (zone_tabs = malloc((sizeof(char) * thread->worker_id * DDZONE_DATA_COLUMS) + 1)) == NULL )
	{
		dd_log(LOG_ERR,"unable to allocate memory for zone_tabs");
		thread->worker_thread_ccode = -1;
		pthread_exit(NULL);
	}

	char *ptr = zone_tabs;
	int i;
	for(i=0; i<thread->worker_id * DDZONE_DATA_COLUMS; i++)
		*ptr++ = '\t';
	*ptr = '\0';

	//
	// determine begin and end seek position given the thread identifier
	//
	u_int64_t pos_start = (parms.read_buffers_per_worker * READ_BUFFER_SIZE) * thread->worker_id;
	u_int64_t pos_end = pos_start + (parms.read_buffers_per_worker * READ_BUFFER_SIZE) - 1;
	if ( last_worker )
		pos_end = parms.source_size_bytes - 1;
	dd_log(LOG_INFO,"ddless initial seek position: %llu, ending position: %llu", pos_start, pos_end);

	//
	// position the source file pointer
	//
	if ( lseek64(thread->source_fd, pos_start, SEEK_SET) == -1 )
	{
		dd_log(LOG_ERR,"seek set to read offset: %llu failed",pos_start);
		thread->worker_thread_ccode = -1;
		pthread_exit(NULL);
	}
	//
	// loop over the data
	//
	int is_done = 0;
	int buffer_read_bytes = 0;
	u_int64_t pos = pos_start;
	gettimeofday(&buffer_time_start,NULL);
	u_int64_t monitor_count = 0;
	while(!is_done)
	{
		if ( (buffer_read_bytes = process_buffer(thread, 
			pos, READ_BUFFER_SIZE)) < 0)
		{
			dd_log(LOG_ERR, "unable to read from source device");
			thread->worker_thread_ccode = -1;
			pthread_exit(NULL);
		}

		if (parms.runmode == RUNMODE_SOURCE_DELTA)
		{
			monitor_count++;
	          	fprintf(parms.delta_info_fd, "Reading block %llu/%llu\n", 1+(long long unsigned int)pos/READ_BUFFER_SIZE, 1+(long long unsigned int)pos_end/READ_BUFFER_SIZE);
			if (monitor_count >= 30) 
			{
				fflush(parms.delta_info_fd);
				monitor_count = 0;
			}
		}

		//
		// EOF (on SunOS we do not get here if rdsk is used!)
		//
		if ( buffer_read_bytes == 0 )
		{
			dd_log(LOG_DEBUG,"buffer_read_bytes is 0, done");
			is_done = 1;
		}
		
		//
		// check for short reads, this should only happen to the last worker
		//
		if ( !last_worker && buffer_read_bytes != READ_BUFFER_SIZE)
		{
			dd_log(LOG_ERR, "expected %d bytes, only got %d bytes, not the last worker!",
				READ_BUFFER_SIZE, buffer_read_bytes);
			thread->worker_thread_ccode = -1;
			pthread_exit(NULL);
		}
		
		pos += buffer_read_bytes;
		if ( pos > pos_end )
		{
			dd_log(LOG_INFO,"current position (%llu), done", pos - 1);
			is_done = 1;
		}

		//
		// ddzone statistics/reader throttle
		//
		// The gettimeofday for the next round is done before the sleep under max_read_mb_sec
		// because the sleep below must be also accounted for. This code assumes that each
		// worker would get the same performance from the underlying device.
		//
		gettimeofday(&buffer_time_end,NULL);

		elapsed_seconds  = buffer_time_end.tv_sec  - buffer_time_start.tv_sec;
		elapsed_useconds = buffer_time_end.tv_usec - buffer_time_start.tv_usec;
		elapsed_mtime = ((elapsed_seconds) * 1000 + elapsed_useconds/1000.0) + 0.5;
		read_mb_sec = (double)((READ_BUFFER_SIZE)/MEGABYTE_FACTOR) / ((double)elapsed_mtime/1000);

		if ( parms.runmode == RUNMODE_DDZONE )
		{
			printf("%u MB %s %ld ms %0.2f MB/s\n", 
				READ_BUFFER_SIZE / MEGABYTE_FACTOR,
				zone_tabs,
				elapsed_mtime,
				read_mb_sec);
			fflush(stdout);
		}

		gettimeofday(&buffer_time_start,NULL);
		
		if ( parms.max_read_mb_sec_per_worker > 0 )
		{
			if ( read_mb_sec > parms.max_read_mb_sec_per_worker )
			{
				sleep_factor_usec += 1000;
			}
			else if( read_mb_sec < parms.max_read_mb_sec_per_worker && sleep_factor_usec > 0)
			{
				sleep_factor_usec -= 1000;
			}
			if ( sleep_factor_usec > 0 )
			{
				usleep(sleep_factor_usec);
			}
			dd_log(LOG_DEBUG,"%u MB %s %ld ms %0.2f MB/s %d usec",
				READ_BUFFER_SIZE / MEGABYTE_FACTOR,
				zone_tabs,
				elapsed_mtime,
				read_mb_sec,
				sleep_factor_usec);
		}

	} // end of read loop

	//
	// ddzone worker cleanup
	//
	if ( parms.runmode == RUNMODE_DDZONE )
	{
		free(zone_tabs);
	}

	pthread_exit(NULL);
}

//-----------------------------------------------------------------------------
// ddless setup and worker thread spawn
//-----------------------------------------------------------------------------
int ddless(int runmode)
{
	thread_struct *thread = NULL;
	parms.runmode = runmode;
	
	//
	// allocated worker thread data structures
	//
	dd_log(LOG_INFO, "segment size: %u bytes", SEGMENT_SIZE);
	dd_log(LOG_INFO, "direct IO: %d", parms.o_direct);
	dd_log(LOG_INFO, "workers: %d", parms.workers);	
	if ( (threads = malloc(sizeof(thread_struct) * parms.workers)) == NULL )
	{
		dd_log(LOG_ERR, "unable to allocated worker space %d bytes",
			sizeof(thread_struct) * parms.workers);
	}
	memset(threads,0,sizeof(thread_struct) * parms.workers);
	
	//
	// initialize them
	//	
	int worker;
	for(worker=0; worker < parms.workers; worker++)
	{
		thread = &threads[worker];
		thread->worker_id = worker;
		if ( ddless_worker_init_reader(thread) == -1 )
		{
			return -1;
		}
	}

	//
	// allocate the ddmap_data object
	//
	if ( *parms.ddmap_dev )
	{
		if ( ( parms.ddmap_data = malloc(sizeof(struct ddmap_data))) == NULL )
		{
			dd_log(LOG_ERR, "unable allocated memory for map");
			return -1;
		}
		memset(parms.ddmap_data, 0, sizeof(struct ddmap_data));
		strncpy(parms.ddmap_data->map_device, parms.ddmap_dev, DEV_NAME_LENGTH);
		
		//
		// read the ddmap
		//
		if ( ddmap_read(parms.ddmap_data,0) )
		{
			dd_log(LOG_ERR, "unable to read ddmap");
			return -1;
		}
		//ddmap_dump(parms.ddmap_data);
	}

	//
	// source device size
	//
	if ((parms.source_size_bytes = dd_device_size(threads[0].source_fd)) == -1 )
	{
		dd_log(LOG_ERR, "unable to determine source device %s size",parms.source_dev);
		return -1;
	}
	dd_log(LOG_INFO, "source device size: %llu bytes (%0.2f GB)", 
		parms.source_size_bytes,
		(float)parms.source_size_bytes/GIGABYTE_FACTOR);

	//
	// calculate number of segments based on source size/SEGMENT_SIZE
	// needed when we create the checksum file, it determines its size.
	//
	parms.source_segments = parms.source_size_bytes / SEGMENT_SIZE;
	parms.source_segment_remainder_bytes = parms.source_size_bytes % SEGMENT_SIZE;
	dd_log(LOG_INFO, "source segments: %llu, remainder: %llu bytes",
		parms.source_segments,parms.source_segment_remainder_bytes );
	if ( parms.source_segment_remainder_bytes > 0 )
	{
		parms.source_segments++;
		dd_log(LOG_INFO, "source segments: %llu (adjusted due to remainder)",
			parms.source_segments);
	}

	//
	// a worker should be able to read data in READ_BUFFER_SIZE chunks. The last
	// worker however can handle any remaining size. If the number of workers *
	// the READ_BUFFER_SIZE is greater than the source, set the number of workers to 1.
	// And only consider this if the number of workers was something other than 1 to
	// begin with.
	//
	if ( parms.workers > 1 && READ_BUFFER_SIZE * parms.workers > parms.source_size_bytes )
	{
		dd_log(LOG_INFO,"number of workers too large because READ_BUFFER_SIZE * workers > source_size_bytes");
		dd_log(LOG_INFO,"reducing workers from %d to 1", parms.workers);
		parms.workers = 1;
	}

	//
	// determine read buffer per entire job (could be off by one) and per worker (last
	// worker will take the remainder of the input file/device), also make sure
	// the numbers are not zero
	//
	u_int64_t read_buffers_per_job = parms.source_size_bytes / READ_BUFFER_SIZE;
	if ( read_buffers_per_job == 0 )
		read_buffers_per_job = 1;

	parms.read_buffers_per_worker = read_buffers_per_job / parms.workers;
	if ( parms.read_buffers_per_worker == 0 )
		parms.read_buffers_per_worker = 1;
		
	dd_log(LOG_INFO,"read buffers/job %llu", read_buffers_per_job);
	dd_log(LOG_INFO,"read buffers/worker %llu", parms.read_buffers_per_worker);

	//
	// read rate controlled?
	//
	if ( parms.max_read_mb_sec > 0 )
	{
		dd_log(LOG_INFO, "source device max read rate: %d MB/s",parms.max_read_mb_sec);
		parms.max_read_mb_sec_per_worker = parms.max_read_mb_sec / (double)parms.workers;
		dd_log(LOG_INFO, "source device max read rate: %0.1f MB/s/worker",parms.max_read_mb_sec_per_worker);
	}
	
	//
	// forced checksum run mode, remove the checksum and let it get created below
	//
	if ( runmode == RUNMODE_CHECKSUM_ONLY )
	{
		dd_log(LOG_INFO,"generate checksum based on input");
		if ( strncmp(parms.checksum_file,"/dev/null",strlen("/dev/null")) == 0 )
		{
			dd_log(LOG_ERR, "unsupported mode: /dev/null and RUNMODE_CHECKSUM_ONLY?");
			return -1;
		}
		if ( dd_file_exists(parms.checksum_file) )
		{
			dd_log(LOG_INFO,"removing existing checksum file: %s",parms.checksum_file);
			if ( unlink(parms.checksum_file) == -1 )
			{
				dd_log(LOG_ERR, "failed to unlink %s",parms.checksum_file);
				return -1;
			}
		}
	}

	//
	// create/open memory mapped file for segment checksums
	//
	if ( runmode != RUNMODE_DDZONE )
	{
		dd_log(LOG_INFO, "checksum file: %s", parms.checksum_file);
		
		//
		// a /dev/null reference means, we will not compute any checksum
		// file related information, the multi-threaded copy process
		// continues however
		//
		if ( strncmp(parms.checksum_file,"/dev/null",strlen("/dev/null")) == 0 )
		{
			dd_log(LOG_INFO,"skipping checksum computations");
			parms.checksum_array = NULL;
		}
		else
		{
			parms.mmap_size = sizeof(checksum_struct) * parms.source_segments;
			dd_log(LOG_INFO, "checksum file size: %llu bytes", parms.mmap_size);

			parms.checksum_file_new = 0;
			if ( !dd_file_exists(parms.checksum_file) ||
				dd_file_size(parms.checksum_file) != parms.mmap_size)
			{
				if ( runmode == RUNMODE_NEW_CHECKSUM )
				{
					dd_log(LOG_INFO,"new checksum file required, exiting...");
					exit(3);
				}
				dd_log(LOG_INFO,"new checksum file required");

				parms.checksum_file_new = 1;
				//
				// If creating a delta we need to retain the old checksum otherwise will have to resend all the data
				//
				if ( ( runmode == RUNMODE_SOURCE_DELTA ) && ( dd_file_exists(parms.checksum_file) ) )
				{
					parms.checksum_file_new = 0;
				}		
			}

			if ( runmode == RUNMODE_NEW_CHECKSUM )
			{
				dd_log(LOG_INFO,"checksum file will be used, exiting");
				return 0;
			}

			if ((parms.mmap_fd = open(parms.checksum_file, O_CREAT|O_RDWR|O_LARGEFILE,
					(mode_t)0600)) == -1 )
			{
				dd_log(LOG_ERR, "unable to open checksum file: %s",parms.checksum_file);
				return -1;
			}

			//
			// set length of file, causing it on create to be a sparse file checksum file
			// and this is required for mmap'd files, else you have a bus error!
			//
			if ( ftruncate64(parms.mmap_fd,parms.mmap_size) )
			{
				dd_log(LOG_ERR, "ftruncate64 of checksum file %s failed to %llu bytes",
					parms.checksum_file,parms.mmap_size);
				return -1;
			}

			//
			// mmap checksum file
			//
			parms.checksum_array = (void *)mmap64(
				0, parms.mmap_size, PROT_READ | PROT_WRITE, MAP_SHARED, parms.mmap_fd, 0);
			if ( parms.checksum_array == MAP_FAILED )
			{
				dd_log(LOG_ERR, "unable to mmap");
				return -1;
			}
			dd_log(LOG_INFO,"checksum array ptr: %p", parms.checksum_array);
		}
	/* sample mmap write code (check for errors)
		u_int64_t i;
		checksum_struct *checksum_ptr;
		checksum_ptr = parms.checksum_array;
		for(i=0; i < parms.source_segments; i++)
		{
			dd_log(LOG_INFO,"checksum array ptr: %p", checksum_ptr);
			checksum_ptr->checksum1_murmur = 0x12345678;
			checksum_ptr->checksum2_crc32 = 0xa1a2a3a4;
			checksum_ptr++;
			//break;
		}
		munmap((void*)parms.checksum_array, parms.mmap_size);
		close(parms.mmap_fd);
		exit(0);
	*/
	}

	if ( runmode == RUNMODE_SOURCE_DELTA )
	{
		if ((parms.delta_fd = open(parms.delta_file, O_CREAT|O_RDWR|O_LARGEFILE|O_TRUNC,
                                (mode_t)0600)) == -1 )
		{
			dd_log(LOG_ERR, "unable to open delta file: %s", parms.delta_file);
			return -1;
		}
		parms.delta_writes       = 0;
		parms.delta_magic_start  = "beefcake";
		parms.delta_magic_end    = "tailcafe";
		u_int64_t seg_size       = SEGMENT_SIZE;

		// Write the delta header info
		delta_header dheader;
		strncpy(dheader.magic_start, MAGIC_START, sizeof(dheader.magic_start));
		strncpy(dheader.magic_version, MAGIC_VERSION, sizeof(dheader.magic_version));
        	dheader.source_size = parms.source_size_bytes;
        	dheader.check_seg_size = seg_size;
        	dheader.conf_opts = 0;

		if (parms.compressedflag > 0) 
		{
        		dheader.conf_opts += set_dd_flag(DDFLAG_COMPRESSED);
                	dd_log(LOG_INFO,"dheader.conf_opts '%d'", dheader.conf_opts);
		}

		parms.delta_size = 0;
		parms.delta_zip_size = 0;

		if ((write(parms.delta_fd, (void *)&dheader, sizeof(delta_header)))==-1)
        	{
                	dd_log(LOG_ERR,"delta: Failed to write delta_header");
                	exit(1);
        	}

		// Allocate memory to use as the compress buffer
		if (parms.zipbuffer == NULL) 
		{
			u_int64_t bound = compressBound ((u_int64_t) READ_BUFFER_SIZE);
			if ((parms.zipbuffer = malloc(bound)) == NULL)
			{
               			dd_log(LOG_ERR,"delta: Failed to malloc %d bytes", bound);
               			exit(1);
			}
                	dd_log(LOG_INFO,"delta: Success with malloc of %d bytes", bound);
		}
		dd_log(LOG_INFO, "uLongf size %d bytes, u_int64_t size %d bytes", sizeof(uLongf), sizeof(u_int64_t));


                strncpy (parms.delta_info_file, parms.delta_file, strlen(parms.delta_file));
                strncat (parms.delta_info_file, ".winfo", strlen(parms.delta_file)+6);
		parms.delta_info_fd = fopen (parms.delta_info_file, "w+");
		dd_log(LOG_INFO, "uLongf size %d bytes, u_int64_t size %d bytes", sizeof(uLongf), sizeof(u_int64_t));


		//
		// open target device for each worker
		//
		for(worker=0; worker < parms.workers; worker++)
		{
                	// dd_log(LOG_INFO,"delta: ddless_worker_init_delta_writer %d/%d", worker+1, parms.workers);
			thread = &threads[worker];
			thread->worker_id = worker;
			if ( ddless_worker_init_delta_writer(thread) == -1 )
			{
				return -1;
			}
		}

	}


	if ( runmode == RUNMODE_SOURCE_TARGET )
	{
		//
		// support devices and regular files, files are setup sparse, however, the initial
		// copy will make them non-sparse. If the file exists, then it must be of sufficient
		// size - we don't unlink the file, since we don't unlink a device either, we use it
		// and write.
		//
		if ( !dd_file_exists(parms.target_dev) )
		{
			dd_log(LOG_INFO,"target file %s not found, creating it",parms.target_dev);

			int tmp_fd;
			if ((tmp_fd = open(parms.target_dev, O_WRONLY|O_CREAT|O_LARGEFILE, (mode_t)0600)) == -1 )
			{
				dd_log(LOG_ERR, "unable to create target file: %s",parms.target_dev);
				return -1;
			}
			close(tmp_fd);

			//
			// since the target file did not exist, we must work from now on as
			// if the checksum was new (i.e. all data gets written)
			//
			parms.checksum_file_new = 1;
		}

		//
		// open target device for each worker
		//
		for(worker=0; worker < parms.workers; worker++)
		{
			thread = &threads[worker];
			thread->worker_id = worker;
			if ( ddless_worker_init_writer(thread) == -1 )
			{
				return -1;
			}
		}

		//
		// target device size
		//
		if ((parms.target_size_bytes = dd_device_size(threads[0].target_fd)) == -1 )
		{
			dd_log(LOG_ERR, "unable to determine target device %s size",parms.target_dev);
			return -1;
		}
		dd_log(LOG_INFO, "target device size: %llu bytes (%0.2f GB)", 
			parms.target_size_bytes,
			(float)parms.target_size_bytes/GIGABYTE_FACTOR);

		//
		// only check target against source size when a non-regular file is involved
		//
		struct stat64 fstat;
		if ( stat64(parms.target_dev,&fstat) )
		{
			dd_log(LOG_ERR, "stat of target device %s failed",parms.target_dev);
			return -1;
		}
		if ( S_ISREG(fstat.st_mode))
		{
			//
			// regular files are set to a given length via truncate
			//
			if ( ftruncate64(threads[0].target_fd,parms.source_size_bytes) )
			{
				dd_log(LOG_ERR, "ftruncate64 of target device %s failed to %llu bytes",
					parms.target_dev,parms.source_size_bytes);
				return -1;
			}
		}
		else
		{
			//
			// target must be at least as large as source
			//
			if ( parms.target_size_bytes < parms.source_size_bytes )
			{
				dd_log(LOG_ERR, "target size insufficient");
				return -1;
			}
		}
	}

	//
	// launch worker threads...
	//
	dd_log(LOG_INFO,"launching worker threads...");
	time(&parms.start_time);
	for(worker=0; worker < parms.workers; worker++)
	{
		thread = &threads[worker];

		if ( pthread_attr_init(&thread->thread_attributes) != 0 )
		{
			dd_log(LOG_ERR,"pthread_attr_init failed");
			return -1;
		}
		if ( pthread_attr_setdetachstate(&thread->thread_attributes, PTHREAD_CREATE_JOINABLE) != 0)
		{
			dd_log(LOG_ERR,"pthread_attr_setdetachstate failed");
			return -1;
		}

		if ( *parms.ddmap_dev )
		{
			//
			// ddmap
			//
			if ( pthread_create(&thread->worker_thread, &thread->thread_attributes, 
				(void *) ddmap_worker_thread, (void *)thread) != 0 )
			{
				dd_log(LOG_ERR,"pthread_create ddmap failed");
				return -1;
			}
		}
		else
		{
			//
			// ddless, ddzone
			//
			if ( pthread_create(&thread->worker_thread, &thread->thread_attributes, 
				(void *) ddless_worker_thread, (void *)thread) != 0 )
			{
				dd_log(LOG_ERR,"pthread_create ddless failed");
				return -1;
			}
		}
		
		//
		// stagger the workers
		//
		if ( parms.workers > 1 )
			usleep(500);
	}

	//
	// wait for them to complete
	//
	dd_log(LOG_INFO,"workers have been spawned, waiting...");
	for(worker=0; worker < parms.workers; worker++)
	{
		thread = &threads[worker];

		if ( pthread_join(thread->worker_thread, NULL) != 0 )
		{
			dd_log(LOG_ERR,"pthread_join failed");
			return -1;
		}
		if ( thread->worker_thread_ccode == -1 )
		{
			dd_log(LOG_ERR,"thread terminated unexpectantly");
			return -1;
		}
	}
	time(&parms.end_time);

	//
	// close source
	//
	for(worker=0; worker < parms.workers; worker++)
	{
		thread = &threads[worker];
		close(thread->source_fd);
	}

	//
	// close ddmap
	//
	if (parms.ddmap_data)
	{
		if (parms.ddmap_data->map)
			free(parms.ddmap_data->map);
		free(parms.ddmap_data);
	}

	//
	// close memory mapped checksum file (must update the file date/time
	// stamp since mmap does not (http://lkml.org/lkml/2007/2/20/255) and
	// backup programs would then miss the checksum file - shit!)
	//
	munmap((void*)parms.checksum_array, parms.mmap_size);
	close(parms.mmap_fd);
	utime(parms.checksum_file,NULL);

	//
	// close target
	//
	if ( runmode == RUNMODE_SOURCE_TARGET || runmode == RUNMODE_SOURCE_DELTA )
	{
		for(worker=0; worker < parms.workers; worker++)
		{
			thread = &threads[worker];
			close(thread->target_fd);
		}

		if ( runmode == RUNMODE_SOURCE_DELTA )
		{
			delta_footer dfooter;
			dfooter.delta_seg_count = parms.delta_writes;
			dfooter.delta_size = parms.delta_size;
			dfooter.delta_zip_size = parms.delta_zip_size;
			strncpy(dfooter.magic_end, MAGIC_END, sizeof(dfooter.magic_end));

			if ((write(parms.delta_fd, (void *)&dfooter, sizeof(delta_footer)))==-1)
        		{
                		dd_log(LOG_ERR,"delta: Failed to write delta_footer");
                		exit(1);
        		}

			close(parms.delta_fd);
			fclose(parms.delta_info_fd);
			unlink(parms.delta_info_file);
        	}
	}

	//
	// performance summary
	//
	u_int64_t read_buffers = 0;
	u_int64_t changed_segments = 0;
	u_int64_t written_bytes = 0;
	for(worker=0; worker < parms.workers; worker++)
	{
		thread = &threads[worker];
		read_buffers += thread->stats_read_buffers;
		changed_segments += thread->stats_changed_segments;
		written_bytes += thread->stats_written_bytes;
	}
	
	double segment_change_percentage = 
		100*((double)changed_segments/(double)parms.source_segments);

	dd_log(LOG_INFO,"total buffers read: %llu",read_buffers);
	dd_log(LOG_INFO,"found changed segments %llu (%0.2f%%) of %llu segments",
		changed_segments,
		segment_change_percentage,
		parms.source_segments);

	if ( runmode == RUNMODE_SOURCE_TARGET )
	{
		dd_log(LOG_INFO,"wrote %llu bytes (%0.2f GB) to target",
			written_bytes, (double)(written_bytes) / GIGABYTE_FACTOR);
	}

	if ( runmode == RUNMODE_SOURCE_DELTA )
	{
		dd_log(LOG_INFO,"wrote %llu bytes (%0.2f GB) to delta",
			written_bytes, (double)(written_bytes) / GIGABYTE_FACTOR);
	}

	double elapsed_sec = difftime(parms.end_time,parms.start_time);
	
	dd_log(LOG_INFO,"processing time: %0.2f seconds (%0.2f minutes)", 
		elapsed_sec, elapsed_sec/60);
	double processing_perf = (double)(parms.source_size_bytes/1024/1024)/elapsed_sec;

	dd_log(LOG_INFO,"processing performance: %0.2f MB/s",processing_perf);

	//
	// output statistics file
	//
	if ( ( runmode == RUNMODE_SOURCE_TARGET || runmode == RUNMODE_SOURCE_DELTA ) && parms.checksum_array != NULL )
	{
		snprintf(parms.stats_file, DEV_NAME_LENGTH, "%s.stats",parms.checksum_file);
		dd_log(LOG_INFO,"preparing stats file %s", parms.stats_file);
		if ((parms.stats_fd = open(parms.stats_file, O_WRONLY|O_CREAT|O_APPEND, (mode_t)0600)) == -1 )
		{
			dd_log(LOG_ERR, "unable to open stats file: %s",parms.stats_file);
			exit(1);
		}
		char tmp[512];
		struct tm *begin;
		begin = localtime(&parms.start_time);
		snprintf(tmp,512, "%04d-%02d-%02d %02d:%02d:%02d %0.5f segment_change_ratio "
			"%llu bytes_written %0.2f seconds %0.2f MB/s\n",
			1900+begin->tm_year,begin->tm_mon+1,begin->tm_mday,
			begin->tm_hour,begin->tm_min,begin->tm_sec,
			segment_change_percentage/100,
			(long long unsigned)written_bytes,
			elapsed_sec,
			processing_perf);
		write(parms.stats_fd,tmp,strlen(tmp));
		close(parms.stats_fd);
	}
	
	//
	// dump to stderr dd like stats (can only report blocks read, the
	// amount written varies
	//
	fprintf(stderr,"%llu MB processed\n", 
		(long long unsigned)parms.source_size_bytes / MEGABYTE_FACTOR);

	return 0;
}

//-----------------------------------------------------------------------------
// display configuration parameters
//-----------------------------------------------------------------------------
void dd_parms()
{
	printf("AUTHOR=Steffen Plotner\n");
	printf("RELEASE_DATE=%s\n",RELEASE_DATE);
	printf("READ_BUFFER_SIZE_BYTES=%d\n",READ_BUFFER_SIZE);
	printf("SEGMENT_SIZE_BYTES=%d\n",SEGMENT_SIZE);
}

//-----------------------------------------------------------------------------
// help
//-----------------------------------------------------------------------------
void usage()
{
	printf(
"ddless by Steffen Plotner, release date: "RELEASE_DATE"\n"
"\n"
"Copy source to target keeping track of the segment checksums. Subsequent\n"
"copies are faster because we assume that not all of the source blocks change.\n"
"\n"
"	ddless	[-d] -s <source> [-m ddmap ][-r <read_rate_mb_s>] -c <checksum>\n"
"		[-b] -t <target> [-w #] [-v]\n"
"\n"
"Produce a checksum file using the specified device. Hint: the device could be\n"
"source or target. Use the target and a new checksum file, then compare it to\n"
"the existing checksum file to ensure data integrity of the target.\n"
"\n"
"	ddless	[-d] -s <source> -c <checksum> [-v]\n"
"\n"
"Determine disk read speed zones, outputs data to stdout.\n"
"\n"
"	ddless	[-d] -s <source> [-v]\n"
"\n"
"Outputs the built in parameters\n"
"\n"
"	ddless	-p\n"
"\n"
"Parameters\n"
"	-d	direct io enabled (i.e. bypasses buffer cache)\n"
"\n"
"	-s	source device\n"
"	-r	max read rate of source device in megabytes/sec\n"
"	-c	checksum file (/dev/null skips checksum file)\n"
"	-b	bail out with exit code 3 because a new checksum file is\n"
"		required, no data is copied from source to target\n"
"	-t	target device\n"
"	-w	number of worker threads, each thread gets a region of the device\n"
"\n"
"	-p	display parameters (segment size is known as chunksize in LVM2)\n"
"	-v	verbose\n"
"	-z	zip the delta file\n"
"	-l	zip level 1 - 9\n"
"	-vv	verbose+debug\n"
"\n"
"Exit codes:\n"
"	0	successful\n"
"	1	a runtime error code, unable to complete task (detailed perror\n"
"		and logical error message are output via stderr)\n"
"	2	reserved\n"
"	3	new checksum file required, only returned if -b is specified, lvm_tools\n"
);
}

//-----------------------------------------------------------------------------
// main
//-----------------------------------------------------------------------------
int main(int argc, char *argv[])
{
	int c, errflg;
	extern char *optarg;
	extern int optind, optopt;

	dd_log_init("ddless");
	int bail_on_new_checksum = 0;
	parms.o_direct           = 0;
	parms.workers            = 1;
	parms.registeredflag     = 0;
	parms.compressedflag     = 0;
	parms.encryptedflag      = 0;
	parms.ziplevel           = 6;
	parms.zipbuffer          = NULL;
	int workers_override     = 0;
	errflg = 0;
	while ((c = getopt(argc, argv, "ds:r:c:bt:x:w:hvpm:zl:")) != -1)
	{
		switch (c)
		{
			case 'd':
				parms.o_direct = 1;
				break;
			case 's':
				strncpy(parms.source_dev, optarg, DEV_NAME_LENGTH);
				break;
			case 'm':
				strncpy(parms.ddmap_dev, optarg, DEV_NAME_LENGTH);
				break;
			case 'r':
				sscanf(optarg,"%d", &parms.max_read_mb_sec);
				break;
			case 'c':
				strncpy(parms.checksum_file, optarg, DEV_NAME_LENGTH);
				break;
			case 'b':
				bail_on_new_checksum = 1;
				break;
			case 't':
				strncpy(parms.target_dev, optarg, DEV_NAME_LENGTH);
				break;
			case 'x':
				strncpy(parms.delta_file, optarg, DEV_NAME_LENGTH);
				break;
			case 'w':
				sscanf(optarg,"%d", &parms.workers);
				if ( parms.workers < 1 )
				{
					dd_log(LOG_ERR,"worker parameter must be >=1");
					exit(1);
				}
				workers_override = 1;
				break;
			case 'v':
				dd_loglevel_inc();
				break;
			case 'p':
				dd_parms();
				exit(0);
			case 'z':
				parms.compressedflag = 1;
				break;
			case 'l':
				sscanf(optarg,"%d", &parms.ziplevel);
				if ( parms.ziplevel < 1 )
				{
					dd_log(LOG_ERR,"ziplevel parameter must be >=1");
					exit(1);
				}
				if ( parms.ziplevel > 9 )
				{
					dd_log(LOG_ERR,"ziplevel parameter must be <=9");
					exit(1);
				}
				break;
			case 'h':
			case '?':
				errflg++;
		}
	}
	if (errflg)
	{
		usage();
		exit(1);
	}

	if ( *parms.source_dev && 
		*parms.checksum_file &&
		*parms.target_dev )
	{
		if ( bail_on_new_checksum )
		{
			dd_log(LOG_INFO,"invoking ddless and verify checksum usability...");
			if ( ddless(RUNMODE_NEW_CHECKSUM) == -1 )
			{
				exit(1);
			}
		}
		else
		{
			dd_log(LOG_INFO,"invoking ddless...");
			if ( ddless(RUNMODE_SOURCE_TARGET) == -1 )
			{
				exit(1);
			}
		}
	}
        else if ( *parms.source_dev &&
                  *parms.checksum_file &&
                  *parms.delta_file )
        {
			// for ddplus, set the workers to one (single thread) when generating delta file
                        dd_log(LOG_INFO,"invoking ddless delta...");
			parms.workers = 1;
                        if ( ddless(RUNMODE_SOURCE_DELTA) == -1 )
                        {
                                exit(1);
                        }
        }

	else if ( *parms.source_dev && 
		*parms.checksum_file )
	{
		dd_log(LOG_INFO,"invoking ddless checksum generation...");
		if ( ddless(RUNMODE_CHECKSUM_ONLY) == -1 )
		{
			exit(1);
		}
	}
	else if ( *parms.source_dev )
	{
		//
		// for ddzone, set the workers to one (single thread) unless
		// the user override the -w parameter
		//
		if ( !workers_override )
			parms.workers = 1;
		dd_log(LOG_INFO,"invoking ddzone...");
		if ( ddless(RUNMODE_DDZONE) == -1 )
		{
			exit(1);
		}
	}
	else
	{
		usage();
		exit(1);
	}
	exit(0);
}

