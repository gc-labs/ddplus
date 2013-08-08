/*
  ddprofile
  
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
int ddprofile(int runmode)
{
	parms.runmode = runmode;

	if ((parms.mmap_fd = dd_dev_open_rw(parms.checksum_file, 0)) == -1 )
        {
                dd_log(LOG_ERR, "unable to open file: %s", parms.checksum_file);
                exit (1);
        }
        dd_log(LOG_DEBUG, "opened file: %s", parms.checksum_file);

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

	u_int64_t i;
	u_int64_t checksum_blank = 0;
	u_int64_t check_count = parms.mmap_size / sizeof(checksum_struct);

	checksum_struct *checksum_ptr = parms.checksum_array;

	for (i=0; i < check_count; i++) 
	{
		if (checksum_ptr->checksum1_murmur == 0x68b3db1f && checksum_ptr->checksum2_crc32 == 0xab54d286) {
			checksum_blank++;
		}
		fprintf (stdout, "block %llu/%llu %08x %08x\n", (long long unsigned int)i+1, (long long unsigned int)check_count, checksum_ptr->checksum1_murmur, checksum_ptr->checksum2_crc32);
		checksum_ptr++;
	}

	fprintf (stdout, "blank %llu/%llu %.2f%%\n", (long long unsigned int)checksum_blank, (long long unsigned int)check_count, 100*(float)checksum_blank/check_count);

       	munmap((void*)parms.checksum_array, parms.mmap_size);
       	close(parms.mmap_fd);

        return 0;
}
//-----------------------------------------------------------------------------
// help
//-----------------------------------------------------------------------------
void usage()
{
	printf(
"ddprofile by Graham Houston, release date: "RELEASE_DATE"\n"
"\n"
"Show the number of blank blocks in a checksum file\n"
"\n"
"	ddprofile	-c checksum [-v]\n"
"\n"
"Parameters\n"
"	-c	checksum file\n"
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

	dd_log_init("ddprofile");
	parms.o_direct  = 0;
	// parms.zipflag   = 0;
	// parms.zipbuffer = NULL;
	errflg = 0;

	while ((c = getopt(argc, argv, "a:c:t:x:dvh?")) != -1)
	{
		switch (c)
		{
			case 'c':
				strncpy(parms.checksum_file, optarg, DEV_NAME_LENGTH);
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

	ddprofile(RUNMODE_SHOW_DELTA);  

	exit (0);
}

