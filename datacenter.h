#ifndef DATACENTER_H
#define DATACENTER_H

/*includes*/
#include "global.h"

/*functions*/
int32_t dc_handler();
void dc_init();
int32_t dc_log(FILE *std_strm, char *msg, char *opn_m, char *fnc_m, int32_t errrno_f);
packet_t *encode_packet(int32_t p_type, int32_t p_id, int32_t p_clk, int32_t p_pool);
packet_t *decode_packet(uint8_t *packet_stream);
void *cl_lstn_thread(void *args);
void *dc_lstn_thread(void *args);
void *dc_bcst_thread(void *args);

/*variables*/
int32_t dc_sys_online;
pool_t ticket_pool;
req_queue_t rq;
clk_t this_clk;
dc_t this_dc;
dc_t *dc_sys;
pthread_mutex_t pool_lock;
pthread_mutex_t bcst_lock;

#endif