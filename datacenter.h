#ifndef DATACENTER_H
#define DATACENTER_H

/*includes*/
#include "global.h"

/*functions*/
int32_t dc_handler();
int32_t dc_log(FILE *std_strm, char *msg, char *opn_m, int32_t clk_val, char *fnc_m, int32_t dc_target, int32_t errrno_f);
uint8_t *encode_packet(int32_t p_type, int32_t p_id, int32_t p_clk, int32_t p_pool);
packet_t *decode_packet(uint8_t *packet_stream);
int32_t max_clk(int32_t local_clk, int32_t recvd_clk);
void inspect_packet_stream(uint8_t *packet_stream);
void inspect_packet(packet_t *packet);
void *cl_lstn_thread(void *args);
void *dc_lstn_thread(void *args);
void *dc_bcst_thread(void *args);

/*variables*/
int32_t *dc_bcst_sock_hndl;
int32_t *dc_lstn_sock_hndl;
int32_t *dc_rspd_sock_hndl;
int32_t cl_lstn_sock_hndl;
int32_t cl_rspd_sock_hndl;
int32_t quit_sig;
int32_t msg_delay;
int32_t dc_sys_online;
req_queue_t rq;
pool_t ticket_pool;
clk_t this_clk;
dc_t this_dc;
dc_t *dc_sys;
pthread_mutex_t dc_lstn_lock;
pthread_mutex_t dc_bcst_lock;
pthread_mutex_t pool_lock;
pthread_mutex_t bcst_lock;
pthread_mutex_t quit_lock;

#endif