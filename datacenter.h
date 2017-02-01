#ifndef DATACENTER_H
#define DATACENTER_H

/*includes*/
#include "global.h"

/*functions*/
int32_t dc_handler();
void dc_init();
void *cl_lstn_thread(void *args);
void *dc_lstn_thread(void *args);
void *dc_recv_thread(void *args);
void *dc_bcst_thread(void *args);
void *dc_send_thread(void *args);

/*variables*/
int32_t ticket_pool;
req_queue_t rq;
clk_t this_clk;
dc_t this_dc;
dc_t *dc_sys;
pthread_mutex_t pool_lock;
pthread_mutex_t bcst_lock;

#endif