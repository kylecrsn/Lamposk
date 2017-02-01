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
#include <signal.h>
#include <errno.h>
#include <netdb.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <libconfig.h>

/*structs*/
typedef struct packet
{
	uint8_t flag;
	uint32_t id;
	uint32_t clk;
	uint32_t pool;
}__attribute__((packed)) packet;
typedef struct req_queue_t
{
	packet **packets;
	int32_t size;
	int32_t head;
	int32_t tail;
	pthread_mutex_t lock;
}req_queue_t;
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
}dc_t;
typedef struct cl_lstn_arg_t
{
	int32_t port;
}cl_lstn_arg_t;
typedef struct dc_bcst_arg_t
{
	int32_t count;
	int32_t port;
}dc_bcst_arg_t;
typedef struct ret_t
{
	int32_t ret;
}ret_t;

/*functions*/
void global_init();
void terminate_handler(int32_t x);
void delay(uint32_t seconds);
struct flock *lock_cfg(int32_t fd);
int8_t unlock_cfg(int32_t fd, struct flock *fl);
void print_tickets(uint32_t amnt);

/*variables*/
char *err_m;
char *log_m;
char *cls_m;

#endif