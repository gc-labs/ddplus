/*
 * (C) 2010 Steffen Plotner <swplotner@amherst.edu>
 * This code is licenced under the GPL.
 */

#ifndef __DDMAP_H__
#define __DDMAP_H__

#define DDMAP_SEGMENT_SIZE 16 * 1024	/* 16384 */
#define DDMAP_SEGMENT_SHIFT 14		/* 2^14 = 16384  */
#define DDMAP_SEGMENT_MASK 0x3FFF	/* (2^14)-1 */
#define DDMAP_U32_SHIFT 5		/* 2^5 = 32 bits */
#define DDMAP_U32_SIZE sizeof(u_int32_t)	/* each u32 word is 4 bytes */
#define DDMAP_512K_SHIFT 19		/* 2^19 = 524288 (512k) */
#define DDMAP_MAP_BIT_EXP 0x7C000	/* bits 2^14 ... 2^18 make up the map bit exponent */

struct ddmap_header {		/* structure aligns to 32 bit words */
	char info[8];		/* ddmap ascii header */
	u_int8_t version;	/* version */
	u_int8_t suspended;	/* was map in a suspended state while reading map contents */
	u_int8_t unused1;
	u_int8_t unused2;
	u_int32_t name_sum;	/* sum of characters of target.lun */
	u_int32_t map_size;	/* units are in u32 *map */
};

struct ddmap_data {
	char map_device[DEV_NAME_LENGTH];
	u_int32_t name_sum;		/* sum of characters of target.lun */
	u_int32_t map_size;		/* units are in u32 *map */
	u_int64_t map_size_bytes;	/* actual number of bytes */
	u_int32_t *map;			/* ddmap of bits */
};

void ddmap_dump(struct ddmap_data *map_data);
int ddmap_read(struct ddmap_data *map_data, int dump_header);

#endif
