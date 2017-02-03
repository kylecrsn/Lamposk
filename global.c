#include "global.h"

void global_init()
{
	terminate_sig = 0;
	signal(SIGINT, terminate_handler);
	opterr = 0;
	cfg_fn = "ciosk.cfg";
	err_m = "[ERR|CLK:";
	log_m = "[LOG|CLK:";
	cls_m = "]:";
}

void terminate_handler(int32_t x)
{
	fprintf(stdout, "\n[SHUTTING DOWN]\n");
	exit(0);
}

void fflush_out_err()
{
	fflush(stdout);
	fflush(stderr);
}

void delay(uint32_t seconds)
{
	uint32_t delay_time = time(0) + seconds;
	while (time(0) < delay_time);
}

struct flock *lock_cfg(int32_t fd)
{
	struct flock *fl = (struct flock *)malloc(sizeof(struct flock));
	fl->l_type = F_WRLCK;
	fl->l_whence = SEEK_SET;
	fl->l_start = 0;
	fl->l_len = 0;
	fl->l_pid = getpid();

	//lock the config file for writing
	if(fcntl(fd, F_SETLKW, fl) < 0)
	{
		fprintf(stderr, "%s%d%s (lock_cfg) something went wrong while attempting to lock the .cfg file (errno: %s)\n", 
			err_m, 0, cls_m, strerror(errno));
		return NULL;
	}

	return fl;
}

int8_t unlock_cfg(int32_t fd, struct flock *fl)
{
	fl->l_type = F_UNLCK;

	//unlock the config file
	if(fcntl(fd, F_SETLK, fl) < 0)
	{
		fprintf(stderr, "%s%d%s (unlock_cfg) something went wrong while attempting to unlock the .cfg file (errno: %s)\n", 
			err_m, 0, cls_m, strerror(errno));
		return -1;
	}

	free(fl);
	return 0;
}

void print_tickets(uint32_t amnt)
{
	if(amnt == 1)
	{
		fprintf(stdout, "ticket");
	}
	else
	{
		fprintf(stdout, "tickets");
	}
}