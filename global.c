#include "global.h"

void global_init()
{
	opterr = 0;
	err_m = "[ERR|CLK:";
	log_m = "[LOG|CLK:";
	cls_m = "]:"
}

void delay(uint32_t seconds)
{
	uint32_t delay_time = time(0) + seconds;
	while (time(0) < delay_time);
}

struct flock *lock_cfg(FILE *fd)
{
	int32_t fd;
	struct flock *fl = (struct flock *fl)malloc(sizeof(struct flock));
	*fl.l_type = F_WRLCK;
	*fl.l_whence = SEEK_SET;
	*fl.l_start = 0;
	*fl.l_len = 0;
	*fl.l_pid = getpid();

	//lock the config file for writing
	if(fcntl(fd, F_SETLKW, fl) < 0)
	{
		fprintf(stderr, "%ssomething went wrong while attempting to lock the .cfg file (errno: %d)\n", err_msg, errno);
		return NULL;
	}

	return fl;
}

int8_t unlock_cfg(FILE *fd, struct flock *fl)
{
	*fl.l_type = F_UNLCK;

	//unlock the config file
	if(fcntl(fd, F_SETLK, fl) < 0)
	{
		fprintf(stderr, "%ssomething went wrong while attempting to unlock the .cfg file (errno: %d)\n", err_msg, errno);
		return -1;
	}

	free(fl);
	return 0;
}

void print_tickets(int32_t x)
{
	if(x == 1)
	{
		fprintf(stdout, "ticket");
	}
	else
	{
		fprintf(stdout, "tickets");
	}
}