#ifndef DATACENTER_H
#define DATACENTER_H

/*includes*/
#include "global.h"

/*functions*/
int32_t datacenter_handler();
void *cl_lstn_thread(void *args);
void *dc_lstn_thread(void *args);
void *dc_bcst_thread(void *args);

#endif