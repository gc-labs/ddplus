/*
  ddless: utilities
  Steffen Plotner, 2008
*/
#include "dd_log.h"

int log_level = 0;
pthread_t main_thread;
#define LOG_NAME_SIZE 128
char log_name[LOG_NAME_SIZE];

void dd_loglevel_inc()
{
	log_level++;
}

void dd_log_init(char *progam_name)
{
	main_thread = pthread_self();
	snprintf(log_name, LOG_NAME_SIZE, "%s", progam_name);
}

void dd_log(int log_type, char *format_string, ...)
{
	va_list var_args;
	
	va_start(var_args, format_string);
	
	//
	// actual logging is determined by loglevel, include thread
	// id when caller is not main().
	//
	if ( log_type < log_level || log_type == LOG_ERR )
	{
		pthread_t pthread = pthread_self();
		if ( pthread != main_thread )
		{
			printf("%s[%x]> ", log_name, (unsigned int)pthread);
		}
		else
		{
			printf("%s> ", log_name);
		}
		vfprintf(stdout, format_string, var_args);
		printf("\n");
	}
	
	//
	// errors always appear
	//
	if ( log_type == LOG_ERR )
	{
		perror("err> perror");
	}

	va_end(var_args);
}
