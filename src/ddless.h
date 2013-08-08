#ifndef DD_LESS_INCLUDED
#define DD_LESS_INCLUDED

//
// C includes
//

// needed for lseek64
#ifndef _LARGEFILE64_SOURCE
	#define _LARGEFILE64_SOURCE
#endif

#include <sys/types.h>
#ifdef SUNOS
	#include <fcntl.h>
	typedef uint8_t u_int8_t;
	typedef uint32_t u_int32_t;
	typedef uint64_t u_int64_t;
#else
	#include <asm/fcntl.h>
#endif

#include <unistd.h>
#include <sys/stat.h>
#include <stdarg.h>
#include <sys/mman.h>
#include <time.h>
#include <string.h>
#include <stdio.h>
#include <pthread.h>
#include <utime.h>
#include <sys/time.h>
#include <stdlib.h>
#include <zlib.h>

extern int open(const char *pathname, int flags, ...);

//
// ddless structures and definitions
//
#define RELEASE_DATE  "2012.10.20"
#define MAGIC_VERSION   "   v2.01"
#define MAGIC_START     "beefcake"
#define MAGIC_END       "tailcafe"

//
// DO NOT CHANGE THE SEGMENT_SIZE AS IT IS USED BY OTHER
// SOFTWARE SUCH AS LINUX IET KERNEL CODE, FOR EXAMPLE!
//
#define READ_BUFFER_SIZE (8 * 1024 * 1024)
#define SEGMENT_SIZE (16*1024)
#define BUFFER_SEGMENTS READ_BUFFER_SIZE/SEGMENT_SIZE

#define GIGABYTE_FACTOR (1024 * 1024 * 1024)
#define MEGABYTE_FACTOR (1024 * 1024)

#define RUNMODE_SOURCE_TARGET 0
#define RUNMODE_CHECKSUM_ONLY 1
#define RUNMODE_NEW_CHECKSUM  2
#define RUNMODE_DDZONE        3
#define RUNMODE_SOURCE_DELTA  4
#define RUNMODE_SHOW_DELTA    5
#define RUNMODE_APPLY_DELTA   6

#define DEV_NAME_LENGTH    1024
#define MAX_CMD_LENGTH     1024

#define DDFLAG_REGISTERED     0
#define DDFLAG_COMPRESSED     1
#define DDFLAG_ENCRYPTED      2

//
// checksum structure (can accomodate multiple algorithms)
//
typedef struct
{
	u_int32_t	checksum1_murmur;
	u_int32_t	checksum2_crc32;
} checksum_struct;


//
// instead of global variables we use a structure
//
typedef struct
{
	int		runmode;
	int		o_direct;
	int		workers;
	unsigned char	registeredflag;
	unsigned char	compressedflag;
	unsigned char	encryptedflag;
	int		ziplevel;
	void	      	*zipbuffer;

	// source device (note that the source_fd is part of thread structure below)
	char		source_dev[DEV_NAME_LENGTH];
	off64_t		source_size_bytes;
	u_int64_t	source_segments;
	u_int64_t	source_segment_remainder_bytes;
	u_int64_t	read_buffers_per_worker;
	int		max_read_mb_sec;
	double		max_read_mb_sec_per_worker;

	// ddmap (each bit indicates a 16KB segment change to be read and written to destination)
	char		ddmap_dev[DEV_NAME_LENGTH];
	struct 		ddmap_data *ddmap_data;

	// memory mapped checksum files
	char		checksum_file[DEV_NAME_LENGTH];
	int		checksum_file_new;
	u_int64_t	mmap_size;
	int		mmap_fd;
	checksum_struct *checksum_array;

	// statistics file
	char		stats_file[DEV_NAME_LENGTH];
	int		stats_fd;
	
	// target device/file
	char		target_dev[DEV_NAME_LENGTH];
	off64_t		target_size_bytes;
	
	// delta file
	char		delta_file[DEV_NAME_LENGTH];
	int		delta_fd;
	u_int64_t	delta_writes;
	char *		delta_magic_start;
	char *		delta_magic_end;
	u_int64_t	delta_size;
	u_int64_t	delta_zip_size;
	u_int64_t	delta_size_bytes;
	
	// delta info file
	char		delta_info_file[DEV_NAME_LENGTH];
	FILE *		delta_info_fd;

	// delta show | apply
	char		delta_action[DEV_NAME_LENGTH];
	
	// timing
	time_t		start_time;
	time_t		end_time;
} parms_struct;

//
// Local thread/buffer structure, there are n buffers that are read then 
// processed by ddless to compute the checksum and perform any necessary writes.
//
typedef struct
{
	//
	// work buffers for reading the source dev
	//
	int	worker_id;
	void	*aligned_buffer;
	int	source_fd;
	int	target_fd;

	//
	// pthread info
	//
	pthread_attr_t	thread_attributes;
	pthread_t	worker_thread;
	int		worker_thread_ccode;

	//
	// track which segment within a buffer has changes (for a 1MB buffer and
	// 16K segment we have 64 segments). One segment tagged at the end -> EOS
	//
	int	seg_bytes_dirty_map[BUFFER_SEGMENTS+1];

	//
	// statistics (at the end the stats are summed up across all workers)
	//
	u_int64_t	stats_read_buffers;
	u_int64_t	stats_changed_segments;
	u_int64_t	stats_written_bytes;
} thread_struct;

typedef struct
{
	char		magic_start[8];
	char		magic_version[8];
        u_int64_t	source_size;
        u_int64_t	check_seg_size;
        u_int64_t	conf_opts;
} delta_header;


typedef struct
{
	u_int64_t	delta_seg_count;
	u_int64_t	delta_size;
	u_int64_t	delta_zip_size;
	char		magic_end[8];
} delta_footer;
#endif
