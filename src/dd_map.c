/*
  ddmap

  Steffen Plotner, 2010
*/

#include "ddless.h"
#include "dd_map.h"
#include "dd_log.h"
#include "dd_file.h"

//-----------------------------------------------------------------------------
// dump the map
//-----------------------------------------------------------------------------
void ddmap_dump(struct ddmap_data *map_data)
{
	int cols = 8;
	int i;
	u_int32_t *ptr = map_data->map;
	char *line;	

	line = malloc(132);
	if (!line)
		return;
	*line = '\0';
	
	for (i=0; i<map_data->map_size; i++)
	{
		if (i % cols == 0 && *line)
		{
			printf("%08llx: %s\n", (long long unsigned)i * DDMAP_U32_SIZE - (cols * DDMAP_U32_SIZE), line);
			*line = '\0';
		}
		snprintf(line+strlen(line), 10, "%08x ",*ptr);
		ptr++;
	}
	printf("%08llx: %s\n", (long long unsigned)i * DDMAP_U32_SIZE, line);
	free(line);
}

//-----------------------------------------------------------------------------
// read the map
//-----------------------------------------------------------------------------
int ddmap_read(struct ddmap_data *map_data, int dump_header)
{
	int ret = 0;
	int fd;

	struct ddmap_header *hdr = NULL;
	int header_bytes = sizeof(struct ddmap_header);
	int read_bytes;
	
	if ((fd = dd_dev_open_ro(map_data->map_device, 0)) == -1 )
	{
		dd_log(LOG_ERR, "unable to open map: %s",map_data->map_device);
		return -1;
	}
	dd_log(LOG_INFO, "opened map: %s",map_data->map_device);
	dd_log(LOG_INFO, "header size: %d", header_bytes);

	if ( (hdr = malloc(header_bytes)) == NULL )
	{
		dd_log(LOG_ERR,"unable to allocate memory for ddmap_header");
		return -1;
	}
	
	if ( (read_bytes = read(fd, hdr, header_bytes)) == -1 )
	{
		dd_log(LOG_ERR, "unable to read header from map");
		ret = -1;
		goto err;
	}

	if ( read_bytes != header_bytes )
	{
		dd_log(LOG_ERR, "unable to read %d header bytes, got %d bytes",
			header_bytes, read_bytes);
		ret = -1;
		goto err;
	}
	
	map_data->name_sum = hdr->name_sum;
	map_data->map_size = hdr->map_size;
	map_data->map_size_bytes = hdr->map_size * DDMAP_U32_SIZE;

	if ( dump_header )
	{
		printf("ddmap.device: %s\n", map_data->map_device);
		printf("ddmap.info: %s\n", hdr->info);
		printf("ddmap.version: %d\n", hdr->version);
		printf("ddmap.suspended: %d\n", hdr->suspended);
		printf("ddmap.name_sum: 0x%08x\n", hdr->name_sum);
		printf("ddmap.map_size: %u (u32)\n", hdr->map_size);
		printf("ddmap.map_size_bytes: %llu\n", (long long unsigned)map_data->map_size_bytes);
	}
	
	
	if ( (map_data->map = malloc(map_data->map_size_bytes)) == NULL )
	{
		dd_log(LOG_ERR, "unable to allocate memory for map");
		ret = -1;
		goto err;
	}

	if ( (read_bytes = read(fd, map_data->map, map_data->map_size_bytes)) == -1 )
	{
		dd_log(LOG_ERR, "unable to read map");
		ret = -1;
		goto err;
	}

	if ( read_bytes != map_data->map_size_bytes )
	{
		dd_log(LOG_ERR, "unable to read %d map bytes, got %d bytes (map data is broken!)",
			map_data->map_size_bytes, read_bytes);
		ret = -1;
		goto err;
	}

err:
	if (hdr)
		free(hdr);
	
	return ret;
}
