/*
  ddless: file io
*/
#ifndef DD_FILE_INCLUDED
#define DD_FILE_INCLUDED

#include "ddless.h"

int dd_dev_open_ro(char *device_name, int o_direct);
int dd_dev_open_rw(char *device_name, int o_direct);
off64_t dd_device_size(int fd);
int dd_file_exists(char *filename);
off64_t dd_file_size(char *filename);
u_int64_t set_dd_flag(u_int64_t flag);

#endif
