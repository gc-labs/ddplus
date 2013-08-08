#
# ddplus makefile
#

PROJECT=ddplus ddcommit ddprofile

OBJS_DDLESS = dd_file.o dd_murmurhash2.o dd_log.o dd_map.o

OBJS_DDMAP = ddmap.o dd_file.o dd_log.o dd_map.o

CC=gcc
CFLAGS=-O3 -Wall $(DEBUG)

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

ddplus: $(OBJS_DDLESS) ddless.o
	mkdir -p bindir
	$(CC) $(CFLAGS) $(OS_CFLAGS) -o bindir/$@ $(OBJS_DDLESS) ddless.o ${LIBS} -s -static

ddcommit: $(OBJS_DDLESS) ddcommit.o
	mkdir -p bindir
	$(CC) $(CFLAGS) $(OS_CFLAGS) -o bindir/$@ $(OBJS_DDLESS) ddcommit.o ${LIBS} -s -static

ddprofile: $(OBJS_DDLESS) ddprofile.o
	mkdir -p bindir
	$(CC) $(CFLAGS) $(OS_CFLAGS) -o bindir/$@ $(OBJS_DDLESS) ddprofile.o ${LIBS} -s -static

clean:
	rm -f bindir/ddplus bindir/ddcommit bindir/ddprofile
	rm -f $(OBJS_DDLESS)
	rm -f $(OBJS_DDMAP)
	rm -f ddcommit.o  ddless.o  ddprofile.o

#
# dependencies
#
dd_file.o: 		dd_file.c dd_file.h ddless.h
dd_murmurhash2.o: 	dd_murmurhash2.h ddless.h
dd_log.o: 		dd_log.h ddless.h
ddless.o: 		ddless.h dd_map.h
ddmap.o: 		dd_map.h
dd_map.o: 		dd_map.h
