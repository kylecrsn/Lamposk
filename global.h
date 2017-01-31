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
}__attribute__((packed)) packet;
typedef struct dc_obj
{
	int32_t id;
	int32_t clk;
	int32_t online;
	char *hostname;
}dc_obj;
typedef struct arg_struct
{
	int32_t port;
	char *hostname;
}arg_obj;
typedef struct ret_struct
{
	int32_t ret;
}ret_obj;

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