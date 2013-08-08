/*
  ddless: file io
  Steffen Plotner, 2008
*/
#include "dd_file.h"

//
// open device readonly and direct (bypass page cache)
//
int dd_dev_open_ro(char *device_name, int o_direct)
{
	#ifdef O_DIRECT
	if ( o_direct )
		return open(device_name, O_RDONLY|O_DIRECT|O_LARGEFILE);
	#endif
	return open(device_name, O_RDONLY|O_LARGEFILE);
}

int dd_dev_open_rw(char *device_name, int o_direct)
{
	#ifdef O_DIRECT
	if ( o_direct )
		return open(device_name, O_RDWR|O_DIRECT|O_LARGEFILE);
	#endif
	return open(device_name, O_RDWR|O_LARGEFILE);
}


//
// get device size (over 4GB and we need lseek64), returns bytes
//
off64_t dd_device_size(int fd)
{
	off64_t size_bytes;
	
	size_bytes = lseek64(fd, 0, SEEK_END);
	if ( size_bytes == -1 )
		return -1;
		
	if ( lseek64(fd, 0, SEEK_SET) == -1 )
		return -1;
	
	return size_bytes;
}

//
// file exists
//
int dd_file_exists(char *filename)
{
	return !access(filename, F_OK);
}

//
// file size
//
off64_t dd_file_size(char *filename)
{
	int fd;
	
	if ((fd = open(filename, O_RDONLY|O_LARGEFILE)) == -1 )
		return fd;
	
	return dd_device_size(fd);
}

u_int64_t set_dd_flag(u_int64_t flag)
{
	return (1 << flag);
}

