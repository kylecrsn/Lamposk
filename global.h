#ifndef GLOBALS_H
#define GLOBALS_H

/*includes*/
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdbool.h>
#include <inttypes.h>
#include <getopt.h>
#include <errno.h>
#include <pthread.h>
#include <netdb.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <libconfig.h>

/*structs*/
typedef struct packet
{
	uint8_t instr;
	uint32_t p_id;
	uint32_t p_clk;
	uint32_t t_amt;
}packet __attribute__((packed));

typedef struct dc_obj
{
	uint32_t id;
	uint8_t online;
	char *hostname;
}dc_obj;

typedef struct arg_struct
{
	int id;
	int *clock;
	int *pool;
	int *requested;
	int delay;
	int port;
	int count;
	char *hostname;
	datacenter_obj *datacenters;
}arg_obj;

typedef struct ret_struct
{
	int ret;
}ret_obj;

/*functions*/
void global_init()
void delay(uint32_t seconds);
struct flock *lock_cfg(FILE *fd);
int8_t unlock_cfg(struct flock *fl);

/*variables*/
char *err_msg;
char *log_msg;

#endif