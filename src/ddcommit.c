/*
  ddcommit
  
*/

#include "ddless.h"
#include "dd_log.h"
#include "dd_murmurhash2.h"
#include "dd_file.h"
#include "dd_map.h"

parms_struct parms;

//-----------------------------------------------------------------------------
u_int64_t read_long(int fd)
{

	u_int64_t buff = 0;	
	int read_size = sizeof(u_int64_t);
	int buffer_read_bytes = 0;

	if ((buffer_read_bytes = read(fd, (void *)&buff, read_size))==-1)
        {
		dd_log(LOG_ERR, "read_long: Failed to read %d bytes", read_size);
		exit(1);
        }
	dd_log(LOG_DEBUG, "read_long: read %d bytes", read_size);

	if (buffer_read_bytes != read_size)
	{
		dd_log(LOG_ERR, "read_long: Failed to read %llu bytes, actual read was %llu", read_size, buffer_read_bytes);
		return -1;
	}
	dd_log(LOG_DEBUG,"read_long: Returned %llu", buff);

	return buff;

}
//-----------------------------------------------------------------------------
int check_string(int fd, char *string)
{
	int buffer_read_bytes = 0;
	int read_size = strlen(string);
	char read_buffer[read_size+1];

	if ( (buffer_read_bytes = read(fd, read_buffer, read_size)) == -1 )
	{
		dd_log(LOG_ERR, "check_string: Failed to read");
		return -1;
	}
	if (buffer_read_bytes != read_size)
	{
		dd_log(LOG_ERR, "check_string: Failed to read %llu bytes, actual read was %llu", read_size, buffer_read_bytes);
		return -1;
	}
	dd_log(LOG_DEBUG,"check_string: read '%s'", read_buffer);

	
        return  (strncmp(string, read_buffer, read_size) == 0);
} 
//-----------------------------------------------------------------------------
int read_struct(int fd, char *struct_data, int struct_size, char *struct_name)
{
	int buffer_read_bytes = 0;

	if ( (buffer_read_bytes = read(fd, struct_data, struct_size)) == -1 )
	{
		dd_log(LOG_ERR, "read_struct: Failed to read");
		return -1;
	}
	if (buffer_read_bytes != struct_size)
	{
		dd_log(LOG_ERR, "read_struct: Failed to read %llu bytes, actual read was %llu", struct_size, buffer_read_bytes);
		return -1;
	}
	return (0);
} 
//-----------------------------------------------------------------------------
void write_checksum(Bytef * ptr, checksum_struct *checksum_ptr, u_int64_t csize)
{
	u_int32_t checksum1_murmur;
	u_int32_t checksum2_crc32 = crc32( 0L, Z_NULL, 0 );

	checksum1_murmur = MurmurHash2(ptr, csize, 0xbabeaffe);
	checksum2_crc32 = crc32(checksum2_crc32, ptr, csize);

	checksum_ptr->checksum1_murmur = checksum1_murmur;
	checksum_ptr->checksum2_crc32 = checksum2_crc32;

}
//-----------------------------------------------------------------------------
int open_file_with_size(char *filetype, char *filename, u_int64_t filesize)
{
        int tmp_fd;
	u_int64_t tmp_size;

        if (!dd_file_exists(filename)) 
        {
        	dd_log(LOG_DEBUG,"%s file %s not found, creating it", filetype, filename);

                if ((tmp_fd = open(filename, O_WRONLY|O_CREAT|O_LARGEFILE, (mode_t)0600)) == -1 )
                {
                	dd_log(LOG_ERR, "unable to create %s file: %s", filetype, filename);
                	exit (1);
                }
		close (tmp_fd);
        }

	if ((tmp_fd = dd_dev_open_rw(filename, 0)) == -1 )
        {
                dd_log(LOG_ERR, "unable to open %s file: %s", filetype, filename);
                exit (1);
        }
        dd_log(LOG_DEBUG, "opened %s file: %s",filetype, filename);


	if ((tmp_size = dd_device_size(tmp_fd)) == -1 )
        {
        	dd_log(LOG_ERR, "unable to determine %s file %s size", filetype, filename);
                exit (1);
        }
        dd_log(LOG_DEBUG, "%s file size: %llu bytes (%0.2f MB)",
                filetype, tmp_size,
                (float)tmp_size/MEGABYTE_FACTOR);


        struct stat64 fstat;
        if ( stat64(filename,&fstat) )
        {
        	dd_log(LOG_ERR, "stat of %s file %s failed", filetype, filename);
                        return -1;
        }
        if ( S_ISREG(fstat.st_mode))
        {
                //
                // regular files are set to a given length via truncate
                //
                if ( tmp_size < filesize ) 
		{
                	if ( ftruncate64(tmp_fd, filesize) )
                	{
                		dd_log(LOG_ERR, "ftruncate64 of %s file %s failed to %llu bytes",
                               		filetype, filename, filesize);
                        	exit (1);
                	}
                }
                if ( tmp_size > filesize ) 
		{
                        dd_log(LOG_ERR, "refusing to reduce the size of %s file %s from %llu to %llu bytes", filetype, filename, tmp_size, filesize);
                        exit (1);
        	}
        }
        else
        {
                //
                // target must be at least as large as source
                //
                if ( tmp_size < filesize )
                {
                        dd_log(LOG_ERR, "%s device %s is too small", filetype, filename);
                        exit (1);
                }
        }


	if (tmp_fd < 0) exit (1);
        return (tmp_fd);
}

//-----------------------------------------------------------------------------
int ddcommit(int runmode)
{
	parms.runmode = runmode;
	thread_struct ts;

	parms.delta_magic_start  = "beefcake";
	parms.delta_magic_end    = "tailcafe";

	if ((parms.delta_fd = open(parms.delta_file, O_RDONLY|O_LARGEFILE,
          (mode_t)0600)) == -1 )
	{
		dd_log(LOG_ERR, "unable to open delta file: %s", parms.delta_file);
		return -1;
	}

	if ((parms.delta_size_bytes = dd_device_size(parms.delta_fd)) == -1 ) 
	{
		dd_log(LOG_ERR, "unable to determine delta file %s size",parms.delta_file);
		return -1;
	}
	dd_log(LOG_DEBUG, "delta file size: %llu bytes", parms.delta_size_bytes); 
	
	// u_int64_t seg_size       = SEGMENT_SIZE;


	u_int64_t source_pos = 0;
	void * read_buffer   = NULL;

        #ifdef SUNOS
        if ((read_buffer = memalign(getpagesize(), READ_BUFFER_SIZE)) == NULL )
        #else
        if (posix_memalign((void**)&read_buffer, getpagesize(), READ_BUFFER_SIZE))
        #endif
        {
                dd_log(LOG_ERR, "unable to allocate buffers with READ_BUFFER_SIZE=%d",READ_BUFFER_SIZE);
                return -1;
        }
        dd_log(LOG_DEBUG, "buffer aligned size %d, address: %p", READ_BUFFER_SIZE, read_buffer);

	
	//
	// position the source file pointer
	//
	if ( lseek64(parms.delta_fd, source_pos, SEEK_SET) == -1 )
	{
		dd_log(LOG_ERR,"seek set to read offset: %llu failed",source_pos);
		return -1;
	}
	

	//
	// read data
	//
	// u_int64_t delta_payload = 0;
	u_int64_t bound = compressBound ((u_int64_t) READ_BUFFER_SIZE);

	delta_header dheader;
	delta_footer dfooter;
	
	if ((read_struct(parms.delta_fd, (char *)&dheader, sizeof(delta_header), "delta_header")) == -1)
	{
		return (-1);
	}	

        if ((strncmp(dheader.magic_start, MAGIC_START, sizeof(dheader.magic_start)) != 0))
	{
		dd_log(LOG_ERR, "failed to read magic start from delta file");
		return -1;
	}
	dd_log(LOG_INFO,"read magic start   '%8.8s' from delta file", dheader.magic_start);
	
        if ((strncmp(dheader.magic_version, MAGIC_VERSION, sizeof(dheader.magic_version)) != 0))
	{
		dd_log(LOG_ERR, "failed to read magic version from delta file");
		return -1;
	}
	dd_log(LOG_INFO,"read magic version '%8.8s' from delta file", dheader.magic_version);
	

	if ( lseek64(parms.delta_fd, parms.delta_size_bytes-sizeof(delta_footer), SEEK_SET) == -1 )
	{
		dd_log(LOG_ERR,"seek set to read offset: %llu failed", parms.delta_size_bytes-sizeof(delta_footer));
		return -1;
	}

	if ((read_struct(parms.delta_fd, (char *)&dfooter, sizeof(delta_footer), "delta_footer")) == -1)
	{
		return (-1);
	}	

       if ((strncmp((char *)&dfooter.magic_end, MAGIC_END, sizeof(dfooter.magic_end)) != 0))
	{
		dd_log(LOG_ERR, "failed to read magic end from delta file");
		return -1;
	}
	dd_log(LOG_INFO,"read magic end     '%8.8s' from delta file", dfooter.magic_end);

	parms.registeredflag = 0;
	parms.compressedflag = 0;
	parms.encryptedflag  = 0;

	unsigned char base_opts = (dheader.conf_opts & 0xff);

	if ((base_opts >> DDFLAG_REGISTERED) & 0x1) parms.registeredflag = 1;
	if ((base_opts >> DDFLAG_COMPRESSED) & 0x1) parms.compressedflag = 1;
	if ((base_opts >> DDFLAG_ENCRYPTED ) & 0x1) parms.encryptedflag  = 1;

	dd_log(LOG_INFO, "parms.registeredflag '%s'", parms.registeredflag ? "TRUE": "FALSE");
	dd_log(LOG_INFO, "parms.compressedflag '%s'", parms.compressedflag ? "TRUE": "FALSE");
	dd_log(LOG_INFO, "parms.encryptedflag  '%s'", parms.encryptedflag  ? "TRUE": "FALSE");

 
	if (parms.compressedflag == 0) {
		dd_log(LOG_INFO,"this is a raw delta file");
	}
	else {
		dd_log(LOG_INFO,"this is a zipped delta file");
	}

	// delta_payload = parms.delta_size_bytes - sizeof(delta_header) - sizeof(delta_footer) - (dfooter.delta_seg_count * 16);
	
	fprintf(stdout, "Zipped:             %s\n",  parms.compressedflag ? "True" : "False");
	fprintf(stdout, "Source size:        %llu\n", (long long unsigned)dheader.source_size);
	fprintf(stdout, "Check Seg size:     %llu\n", (long long unsigned)dheader.check_seg_size);
	fprintf(stdout, "Segment count:      %llu\n", (long long unsigned)dfooter.delta_seg_count);
	fprintf(stdout, "Delta size:         %llu\n", (long long unsigned)dfooter.delta_size);
	if (parms.compressedflag == 1) 
	{
		fprintf(stdout, "Delta zip size:     %llu\n", (long long unsigned)dfooter.delta_zip_size);
		if (dfooter.delta_size > 0) 
		{
			fprintf(stdout, "Zip Ratio:         %5.2f%%\n", (float)dfooter.delta_zip_size/dfooter.delta_size);
		}
	}
	// fprintf(stdout, "Delta size(calc):   %llu\n", (long long unsigned)delta_payload);

	if (parms.runmode == RUNMODE_APPLY_DELTA) {

		u_int64_t checksum_size = 0;
		checksum_size = dheader.source_size / dheader.check_seg_size;
		if (dheader.source_size % dheader.check_seg_size > 0) checksum_size++;
		checksum_size = checksum_size * sizeof(checksum_struct);
		fprintf(stdout, "Checksum size:      %llu\n", (long long unsigned)checksum_size);


		if ( lseek64(parms.delta_fd, sizeof(delta_header), SEEK_SET) == -1 )
		{
			dd_log(LOG_ERR,"seek set to read offset: %llu failed", 40);
			return -1;
		}

        	if ( strncmp(parms.checksum_file,"/dev/null",strlen("/dev/null")) == 0 )
               	{
                	dd_log(LOG_INFO,"skipping checksum computations");
                       	parms.checksum_array = NULL;
               	}
		else 
		{
			parms.mmap_fd = open_file_with_size("checksum", parms.checksum_file, checksum_size);

			if ((parms.mmap_size = dd_device_size(parms.mmap_fd)) == -1 )
			{
				dd_log(LOG_ERR, "unable to determine checksum file %s size",parms.checksum_file);
				return -1;
			}

			parms.checksum_array = (void *)mmap64(
				0, parms.mmap_size, PROT_READ | PROT_WRITE, MAP_SHARED, parms.mmap_fd, 0);
			if ( parms.checksum_array == MAP_FAILED )
			{
				dd_log(LOG_ERR, "unable to mmap from the checksum file");
				return -1;
			}
			dd_log(LOG_DEBUG,"checksum array ptr: %p", parms.checksum_array);
		}

		strncpy (parms.delta_info_file, parms.delta_file, strlen(parms.delta_file));
		strncat (parms.delta_info_file, ".rinfo", strlen(parms.delta_file)+6);
		parms.delta_info_fd = fopen (parms.delta_info_file, "w+");

		ts.target_fd = open_file_with_size("target", parms.target_dev, dheader.source_size);
		if (ts.target_fd < 0) { exit (1); }

		// Allocate memory to use as the compress buffer
		if (parms.zipbuffer == NULL) 
		{
			if ((parms.zipbuffer = malloc(bound)) == NULL)
			{
               			dd_log(LOG_ERR,"delta: Failed to malloc %d bytes", bound);
               			exit(1);
			}
                	dd_log(LOG_DEBUG,"delta: Success with malloc of %d bytes", bound);
		}
		dd_log(LOG_DEBUG, "uLongf size %d bytes, u_int64_t size %d bytes", sizeof(uLongf), sizeof(u_int64_t));

		u_int64_t i;
		for (i=0; i < dfooter.delta_seg_count; i++) {
			int buffer_read_bytes = 0;
			u_int64_t seg_offset = read_long(parms.delta_fd);
			u_int64_t data_size  = read_long(parms.delta_fd);
	
			// fprintf (stdout, "Applying data block %lu/%lu, size %lu at offset %lu\n", i+1, dfooter.delta_seg_count, data_size, seg_offset);

			if (parms.compressedflag > 0) 
			{
				dd_log(LOG_DEBUG, "unzipping segment(s)");

				if ( (buffer_read_bytes = read(parms.delta_fd, parms.zipbuffer, data_size)) == -1 )
				{
					dd_log(LOG_ERR, "unable to read %llu compressed bytes from delta file, block %lu of %lu", data_size, i+1, dfooter.delta_seg_count);
					return -1;
				}
		
				// ZEXTERN int ZEXPORT uncompress OF((Bytef *dest, uLongf *destLen, const Bytef *source, uLong sourceLen));
				uLongf destLen = bound;
				int uncomp_ret;
				if ((uncomp_ret = uncompress ((Bytef *)read_buffer, &destLen, (Bytef *)parms.zipbuffer, data_size)) != Z_OK) 
				{
                                       	dd_log(LOG_ERR,"uncompress delta: failed at block %lu of %lu - input size %lu - error %d", i+1, dfooter.delta_seg_count, data_size, uncomp_ret);
                                       	exit(1);

				}
                                dd_log(LOG_DEBUG,"uncompress delta: uncompressed %lu bytes to %lu bytes", data_size, destLen);
				data_size = destLen;

				
			}
			else 
			{
				if ( (buffer_read_bytes = read(parms.delta_fd, read_buffer, data_size)) == -1 )
				{
					dd_log(LOG_ERR, "unable to read %llu bytes from delta file, block %lu of %lu", data_size, i+1, dfooter.delta_seg_count);
					return -1;
				}
		
			}

			u_int64_t delta_bytes_written;

                        if ( lseek64(ts.target_fd, seg_offset, SEEK_SET) == -1 )
                        {
                                dd_log(LOG_ERR,"seek set to write offset: %llu failed",seg_offset);
                                exit(1);
                        }


                        if ((delta_bytes_written = write(ts.target_fd, read_buffer, data_size))==-1)
                        {
                        	dd_log(LOG_ERR,"delta write failed");
                                exit(1);
                        }
                        dd_log(LOG_DEBUG,"Writing block %llu/%llu, size %llu at offset %llu", i+1, dfooter.delta_seg_count, data_size, seg_offset);

                        fprintf(parms.delta_info_fd, "Writing block %llu/%llu, size %llu at offset %llu\n", (long long unsigned)i+1, (long long unsigned)dfooter.delta_seg_count, (long long unsigned)data_size, (long long unsigned)seg_offset);


			if (parms.checksum_array != NULL)
			{

				u_int64_t j;
				Bytef * ptr = read_buffer;
				u_int64_t check_count  = data_size  / dheader.check_seg_size;
				u_int64_t check_offset = seg_offset / dheader.check_seg_size;
				checksum_struct *checksum_ptr = parms.checksum_array + check_offset;

				for (j=0; j < check_count; j++) 
				{
                               		dd_log(LOG_DEBUG,"Writing checksum %llu/%llu in block %llu/%llu", j+1, check_count, i+1, dfooter.delta_seg_count);
					write_checksum(ptr, checksum_ptr, dheader.check_seg_size);
					ptr += dheader.check_seg_size;
					checksum_ptr++;
				}

				// Catch any trailing data
				u_int64_t check_trail = data_size % dheader.check_seg_size;
				if (check_trail > 0) {
                        	        dd_log(LOG_DEBUG,"Writing checksum of %lu trailing bytes in block %lu/%lu", check_trail, i+1, dfooter.delta_seg_count);
					write_checksum(ptr, checksum_ptr, check_trail);
					ptr += check_trail;
					checksum_ptr++;
				}
			}

		}
		// close memory mapped checksum file (must update the file date/time
       	 	// stamp since mmap does not (http://lkml.org/lkml/2007/2/20/255) and
       		// backup programs would then miss the checksum file - shit!)
        	//
               	if (parms.checksum_array != NULL)
		{
        		munmap((void*)parms.checksum_array, parms.mmap_size);
        		close(parms.mmap_fd);
        		utime(parms.checksum_file,NULL);
		}

        	close(ts.target_fd);
		fclose(parms.delta_info_fd);
		unlink(parms.delta_info_file);
	}

	close (parms.delta_fd);

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
"ddcommit by Graham Houston, release date: "RELEASE_DATE"\n"
"\n"
"Apply the delta file to the target and update the checksum file\n"
"\n"
"	ddcommit	[-d] -a <show|apply> -x <delta> -t <target> [-c checksum] [-v]\n"
"\n"
"Parameters\n"
"	-d	direct io enabled (i.e. bypasses buffer cache)\n"
"\n"
"	-a	action - show or apply\n"
"	-c	checksum file\n"
"	-t	target device\n"
"	-v	verbose\n"
"\n"
"Exit codes:\n"
"	0	successful\n"
"	1	a runtime error code, unable to complete task (detailed perror\n"
"		and logical error message are output via stderr)\n"
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

	dd_log_init("ddcommit");
	parms.o_direct           = 0;
        parms.registeredflag     = 0;
        parms.compressedflag     = 0;
        parms.encryptedflag      = 0;
        parms.delta_size_bytes   = 0;
	parms.zipbuffer          = NULL;
	errflg = 0;

	while ((c = getopt(argc, argv, "a:c:t:x:dvh?")) != -1)
	{
		switch (c)
		{
			case 'a':
				strncpy(parms.delta_action, optarg, DEV_NAME_LENGTH);
				break;
			case 'c':
				strncpy(parms.checksum_file, optarg, DEV_NAME_LENGTH);
				break;
			case 't':
				strncpy(parms.target_dev, optarg, DEV_NAME_LENGTH);
				break;
			case 'x':
				strncpy(parms.delta_file, optarg, DEV_NAME_LENGTH);
				break;
			case 'd':
				parms.o_direct = 1;
				break;
			case 'v':
				dd_loglevel_inc();
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

	if (strncmp(parms.delta_action, "show", strlen("show")) == 0) {
	  fprintf(stdout, "Action:             %s\n", parms.delta_action);
	  ddcommit(RUNMODE_SHOW_DELTA);  
        }
	else {
          if (strncmp(parms.delta_action, "apply", strlen("apply")) == 0) {
	    fprintf(stdout, "Action:             %s\n", parms.delta_action);
	    ddcommit(RUNMODE_APPLY_DELTA);  
          }
          else {
	    dd_log(LOG_ERR,"unknown action");
	    exit(1);
          }
        }


	exit(0);
}

