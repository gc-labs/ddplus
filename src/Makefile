#
# ddplus makefile
#

PROJECT=ddplus ddcommit ddprofile

OBJS = dd_file.o dd_murmurhash2.o dd_log.o 

CC=gcc
CFLAGS=-O3 -Wall $(DEBUG)
## STATIC=-static

OS=$(shell uname -s)
ifeq ($(OS),Linux)
CFLAGS += -pthread
endif

ifeq ($(OS),SunOS)
CFLAGS += -DSUNOS
endif

#
# zlib provides crc32 code
#
LIBS=-lz

all: $(PROJECT)

ddplus: $(OBJS) dd_map.o ddless.o
	$(CC) $(CFLAGS) $(OS_CFLAGS)  -o bindir/$@ $(OBJS) dd_map.o ddless.o ${LIBS} -s ${STATIC}

ddcommit: $(OBJS) ddcommit.o
	$(CC) $(CFLAGS) $(OS_CFLAGS)  -o bindir/$@ $(OBJS) ddcommit.o ${LIBS} -s ${STATIC}

ddprofile: $(OBJS) ddprofile.o
	$(CC) $(CFLAGS) $(OS_CFLAGS)  -o bindir/$@ $(OBJS) ddprofile.o ${LIBS} -s ${STATIC}

clean:
	rm -f $(OBJS)
	rm -f dd_map.o ddcommit.o  ddless.o  ddprofile.o
	rm -f bindir/ddplus bindir/ddcommit bindir/ddprofile

#
# dependencies
#
dd_file.o: 		dd_file.c dd_file.h ddless.h
dd_murmurhash2.o: 	dd_murmurhash2.h ddless.h
dd_log.o: 		dd_log.h ddless.h
ddless.o: 		ddless.h dd_map.h
ddmap.o: 		dd_map.h
dd_map.o: 		dd_map.h
