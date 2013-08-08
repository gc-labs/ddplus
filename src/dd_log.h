/*
  ddless: utililties
*/
#ifndef DD_LOG_INCLUDED
#define DD_LOG_INCLUDED

#include "ddless.h"

// incremental verbosity
#define LOG_INFO 0
#define LOG_DEBUG 1
#define LOG_DEBUG2 2

// always show up
#define LOG_ERR 10

void dd_loglevel_inc();
void dd_log_init(char *program_name);
void dd_log(int log_type, char *format_string, ...);

#endif
