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
#include <netdb.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <libconfig.h>

/*macros*/
#define ONLINE 0
#define OFFLINE 1
#define REQUEST 2
#define RELEASE 3
#define ACK 4

/*structs*/
typedef struct packet_t
{
	uint32_t type;
	uint32_t id;
	uint32_t clk;
	uint32_t pool;
}__attribute__((packed)) packet_t;
typedef struct lamport_t
{
	int32_t clk;
	int32_t id;
}lamport_t;
typedef struct req_queue_t
{
	lamport_t *requests;
	int32_t size;
	pthread_mutex_t lock;
}req_queue_t;
typedef struct pool_t
{
	int32_t pool;
	pthread_mutex_t lock;
}pool_t;
typedef struct clk_t
{
	int32_t clk;
	pthread_mutex_t lock;
}clk_t;
typedef struct dc_t
{
	int32_t id;
	int32_t online;
	char *hostname;
	pthread_mutex_t lock;
}dc_t;
typedef struct dc_lstn_arg_t
{
	int32_t src_id;
	int32_t port_base;
}dc_lstn_arg_t;
typedef struct dc_bcst_arg_t
{
	int32_t dst_id;
	int32_t port_base;
}dc_bcst_arg_t;
typedef struct cl_lstn_arg_t
{
	int32_t port;
}cl_lstn_arg_t;
typedef struct ret_t
{
	int32_t ret;
}ret_t;

/*functions*/
void global_init();
void fflush_out_err();
void delay(uint32_t seconds);
struct flock *lock_cfg(int32_t fd);
int8_t unlock_cfg(int32_t fd, struct flock *fl);
void print_tickets(uint32_t amnt);

/*variables*/
char *cfg_fn;
char *err_m;
char *log_m;
char *cls_m;

#endif