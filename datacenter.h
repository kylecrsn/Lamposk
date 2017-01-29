#ifndef DATACENTER_H
#define DATACENTER_H

/*includes*/
#include "global.h"

/*functions*/
int datacenter_handler(config_t *cf, config_setting_t *lock_setting, int msg_delay);
void *client_recv_thread(void *args);
void *datacenter_recv_thread(void *args);
void *datacenter_send_thread(void *args);
void *stdin_thread(void *args);

/*variables*/
int request_sig;
int claimed_sig;
int release_sig;
int terminate_sig;
int l_clock;
int clock_queue[6];
int id_queue[6];

#endif