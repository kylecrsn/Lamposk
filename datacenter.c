#include "datacenter.h"

int datacenter_handler()
{
	request_sig = 0;
	claimed_sig = 0;
	release_sig = 0;
	terminate_sig = 0;
	l_clock = 1;
	int status = 0;
	int tickets_requested = 0;
	int i;
	int ticket_pool;
	int datacenter_count;
	int datacenter_addr_running;
	int datcenter_addr_id;
	int client_recv_port;
	int datacenter_recv_port;
/*	int client_recv_ret_val;
	int datacenter_recv_ret_val;
	int datacenter_send_ret_val;*/
	const char *hostname;
	config_setting_t *datacenter_addr_settings;
	config_setting_t *datacenter_addr_elem_setting;
	config_setting_t *datacenter_addr_running_setting;
	datacenter_obj datacenter;
	datacenter_obj *datacenters;
	arg_obj *client_recv_args;
	arg_obj *datacenter_recv_args;
	arg_obj *datacenter_send_args;
	arg_obj *stdin_args;
/*	ret_obj *client_recv_rets;
	ret_obj *datacenter_recv_rets;
	ret_obj *datacenter_send_rets;*/
	pthread_t client_recv_thread_id;
	pthread_t datacenter_recv_thread_id;
	pthread_t datacenter_send_thread_id;
	pthread_t stdin_thread_id;

	/*
		REVISED VARIABLES
	*/
	int32_t i_dc = -1;
	config_t cf_local;
	config_t *cf;
	struct flock *fl;
	FILE *fd;

	/*
		REVISED CODE
	*/

	//open and lock the config file
	fd = open("global.cfg", O_RDWR);
	fl = lock_cfg(fd);
	if(fl == NULL)
	{
		return 1;
	}

	//initialize the config object
	cf = &cf_local;
	config_init(cf);
	if(!config_read_file(cf, "global.cfg"))
	{
		fprintf(stderr, "%sthe datacenter encountered an issue while reading from config file: %s:%d - %s\n", 
			err_msg, config_error_file(cf), config_error_line(cf), config_error_text(cf));
		config_destroy(cf);
		unlock_cfg(fd, fl);
		close(fd);
		return 1;
	}

	//lookup config values for the datacenter
	config_lookup_int(cf, "delay", &msg_delay);
	config_lookup_int(cf, "datacenter.ticket_pool", &ticket_pool);
	config_lookup_int(cf, "datacenter.client_recv_port", &c_recv_port);
	config_lookup_int(cf, "datacenter.datacenter_recv_port", &dc_recv_port);
	dc_addr_settings = config_lookup(cf, "datacenter.addresses");
	dc_count = config_setting_length(dc_addr_settings);
	dc_sys = (dc_obj *)malloc(dc_count * sizeof(dc_obj));
	for(i = 0; i < dc_count; i++)
	{
		dc_addr_elem_setting = config_setting_get_elem(dc_addr_settings, i);
		config_setting_lookup_int(dc_addr_elem_setting, "id", &(dc_sys[i]->id));
		config_setting_lookup_int(dc_addr_elem_setting, "online", &(dc_sys[i]->online));
		config_setting_lookup_string(dc_addr_elem_setting, "hostname", &hostname);
		dc_sys[i] = (char *)malloc(strlen(hostname) + 1);
		mempcy(dc_sys[i].hostname, hostname, strlen(hostname) + 1);
		//set the index for the first availble datacenter not currently online
		if(dc_sys[i]->online == 0 && i_dc == -1)
		{
			i_dc = i;
		}
	}

	//all datacenters are already online
	if(dc_sys[i-1]->online == 1)
	{
		fprintf(stderr, "%scan't start another datacenter, all datacenters specified by global.cfg are currently online\n", err_msg);
		config_destroy(cf);
		unlock_cfg(fd, fl);
		close(fd);
		return 1;
	}
	dc_addr_online_setting = config_setting_get_member(dc_addr_elem_setting, "online");


	config_setting_set_int(lock_setting, 0);
	config_write_file(cf, "global.cfg");
	config_destroy(cf);
	memset(clock_queue, 9, sizeof(clock_queue));
	memset(id_queue, 9, sizeof(id_queue));

	fprintf(stdout, "%sticket pool: %d\n", log_msg, ticket_pool);
	fprintf(stdout, "%sclient receive port: %d\n", log_msg, client_recv_port);
	fprintf(stdout, "%sdatacenter receive port: %d\n", log_msg, datacenter_recv_port);
	fprintf(stdout, "%sdatacenter ID: %d\n", log_msg, datacenter.id);
	fprintf(stdout, "%sdatcenter hostname: %s\n", log_msg, datacenter.hostname);

	//spawn client_recv thread
	client_recv_args = (arg_obj *)malloc(sizeof(struct arg_struct));
	client_recv_args->id = datacenter.id;
	client_recv_args->pool = &ticket_pool;
	client_recv_args->requested = &tickets_requested;
	client_recv_args->count = datacenter_count;
	client_recv_args->port = client_recv_port;
	client_recv_args->hostname = datacenter.hostname;
	fflush(stdout);
	status = pthread_create(&client_recv_thread_id, NULL, client_recv_thread, (void *)client_recv_args);
	if(status != 0)
	{
		fprintf(stderr, "%sdatacenter %d failed to spawn client_recv thread (status: %d/errno: %d)\n", 
			err_msg, datacenter.id, status, errno);
		return 1;
	}

	//spawn datacenter_recv thread
	datacenter_recv_args = (arg_obj *)malloc(sizeof(struct arg_struct));
	datacenter_recv_args->id = datacenter.id;
	datacenter_recv_args->pool = &ticket_pool;
	datacenter_recv_args->requested = &tickets_requested;
	datacenter_recv_args->delay = msg_delay;
	datacenter_recv_args->count = datacenter_count;
	datacenter_recv_args->port = datacenter_recv_port;
	datacenter_recv_args->hostname = datacenter.hostname;
	fflush(stdout);
	status = pthread_create(&datacenter_recv_thread_id, NULL, datacenter_recv_thread, (void *)datacenter_recv_args);
	if(status != 0)
	{
		fprintf(stderr, "%sdatacenter %d failed to spawn datacenter_recv thread (status: %d/errno: %d)\n", 
			err_msg, datacenter.id, status, errno);
		return 1;
	}

	//spawn datacenter_send thread
	datacenter_send_args = (arg_obj *)malloc(sizeof(struct arg_struct));
	datacenter_send_args->id = datacenter.id;
	datacenter_send_args->pool = &ticket_pool;
	datacenter_send_args->requested = &tickets_requested;
	datacenter_send_args->delay = msg_delay;
	datacenter_send_args->count = datacenter_count;
	datacenter_send_args->port = datacenter_recv_port;
	datacenter_send_args->hostname = datacenter.hostname;
	datacenter_send_args->datacenters = datacenters;
	fflush(stdout);
	status = pthread_create(&datacenter_send_thread_id, NULL, datacenter_send_thread, (void *)datacenter_send_args);
	if(status != 0)
	{
		fprintf(stderr, "%sdatacenter %d failed to spawn datacenter_send thread (status: %d/errno: %d)\n", 
			err_msg, datacenter.id, status, errno);
		return 1;
	}

	//spawn stdin thread
	stdin_args = (arg_obj *)malloc(sizeof(struct arg_struct));
	fflush(stdout);
	status = pthread_create(&stdin_thread_id, NULL, stdin_thread, (void *)stdin_args);
	if(status != 0)
	{
		fprintf(stderr, "%sdatacenter %d failed to spawn stdin thread (status: %d/errno: %d)\n", 
			err_msg, datacenter.id, status, errno);
		return 1;
	}

	fprintf(stdout, "%sfinished spawning threads\n", log_msg);

	//terminate datacenter
	while(1)
	{
		if(terminate_sig == 1)
		{
			fflush(stdout);
			pthread_cancel(client_recv_thread_id);
			pthread_cancel(datacenter_recv_thread_id);
			pthread_cancel(datacenter_send_thread_id);
			pthread_cancel(stdin_thread_id);
		}
	}

	fprintf(stdout, "%sfinished canceling threads\n", log_msg);

	//free the hostname memory
	free(datacenter.hostname);
	for(i = 0; i < datacenter_count; i++)
	{
		free(datacenters[i].hostname);
	}

	//re-init the config variable, lock the file, clear datacenter running flag, unlock the file
	while(1)
	{
		config_init(cf);
		if(!config_read_file(cf, "global.cfg"))
		{
			fprintf(stderr, "%s%s:%d - %s\n", err_msg, config_error_file(cf), config_error_line(cf), config_error_text(cf));
			config_destroy(cf);
			return 1;
		}

		lock_setting = config_lookup(cf, "lock");
		if(config_setting_get_int(lock_setting) == 1)
		{
			config_destroy(cf);
		}
		else
		{
			config_setting_set_int(lock_setting, 1);
			config_write_file(cf, "global.cfg");
			datacenter_addr_settings = config_lookup(cf, "datacenter.addresses");
			for(i = 0; i < datacenter_count; i++)
			{
				datacenter_addr_elem_setting = config_setting_get_elem(datacenter_addr_settings, i);
				config_setting_lookup_int(datacenter_addr_elem_setting, "id", &datcenter_addr_id);
				if(datcenter_addr_id == datacenter.id)
				{
					break;
				}
			}
			datacenter_addr_running_setting = config_setting_get_member(datacenter_addr_elem_setting, "running");
			config_setting_set_int(datacenter_addr_running_setting, 0);
			config_setting_set_int(lock_setting, 0);
			config_write_file(cf, "global.cfg");
			config_destroy(cf);
			break;
		}
	}

	return 0;
}

//act a frontend for communicating with the client
void *client_recv_thread(void *args)
{
	int msg_buf_max = 4096;
	int sockopt = 1;
	int status;
	int id;
	int port;
	int client_recv_sock_fd;
	int client_rspd_sock_fd;
	int *pool;
	int *requested;
	int msg_delay;
	int ticket_amount;
	char msg_buf[msg_buf_max];
	char *hostname;
	char *end;
	struct sockaddr_in client_recv_addr;
	struct sockaddr_in client_rspd_addr;
	socklen_t client_recv_addr_len;
	socklen_t client_rspd_addr_len;
	arg_obj *thread_args = (arg_obj *)args;
	ret_obj *thread_rets = (ret_obj *)malloc(sizeof(ret_obj));

	fflush(stdout);
	id = thread_args->id;
	pool = thread_args->pool;
	requested = thread_args->requested;
	msg_delay = thread_args->delay;
	port = thread_args->port;
	hostname = thread_args->hostname;
	free(thread_args);

	//setup the client_recv address object
	client_recv_addr_len = sizeof(client_recv_addr);
	memset((char *)&client_recv_addr, 0, client_recv_addr_len);
	client_recv_addr.sin_family = AF_INET;
	client_recv_addr.sin_addr.s_addr = inet_addr(hostname);
	client_recv_addr.sin_port = htons(port);

	while(1)
	{
		fprintf(stdout, "%scurrent ticket pool amount: %d\n", log_msg, *pool);

		//open a datacenter socket
		status = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
		if(status < 0)
		{
			fprintf(stderr, "%sdatacenter %d failed to open a socket (status: %d/errno: %d)\n", 
				err_msg, id, status, errno);
			thread_rets->ret = 1;
			break;
		}
		client_recv_sock_fd = status;

		//check if the system is still holding onto the port after a recent restart
		status = setsockopt(client_recv_sock_fd, SOL_SOCKET, SO_REUSEADDR, (const void *)&sockopt , sizeof(int));
		if(status < 0)
		{
			fprintf(stderr, "%sdatacenter %d can't bind to port due to the system still holding the port resources from a recent \
				restart (status: %d/errno: %d)\n", err_msg, id, status, errno);
			thread_rets->ret = 1;
			break;
		}

		//bind to a port on the datacenter
		status = bind(client_recv_sock_fd, (struct sockaddr *)&client_recv_addr, client_recv_addr_len);
		if(status < 0)
		{
			fprintf(stderr, "%sdatacenter %d failed to bind to the specified port (status: %d/errno: %d)\n", 
				err_msg, id, status, errno);
			thread_rets->ret = 1;
			break;
		}

		//listen to the port
		client_rspd_addr_len = sizeof(client_rspd_addr);
		status = listen(client_recv_sock_fd, 16);
		if(status < 0)
		{
			fprintf(stderr, "%sdatacenter %d failed to listen for clients (status: %d/errno: %d)\n", 
				err_msg, id, status, errno);
			thread_rets->ret = 1;
			break;
		}

		//accept connection from client
		status = accept(client_recv_sock_fd, (struct sockaddr *)&client_rspd_addr, &client_rspd_addr_len);
		if(status < 0)
		{
			fprintf(stderr, "%sdatacenter %d failed to accept a connection from a client (status: %d/errno: %d)\n", 
				err_msg, id, status, errno);
			thread_rets->ret = 1;
			break;
		}
		client_rspd_sock_fd = status;

		//wait to receive a message from the client
		memset(msg_buf, 0, msg_buf_max);
		status = recv(client_rspd_sock_fd, msg_buf, msg_buf_max, 0);
		if(status != msg_buf_max)
		{
			fprintf(stderr, "%sdatacenter %d encountered an issue while reading the message (status: %d/errno: %d)\n", 
				err_msg, id, status, errno);
			thread_rets->ret = 1;
			break;
		}

		//convert message to an int
		errno = 0;
		ticket_amount = strtol(msg_buf, &end, 10);
		if(*end != 0 || errno != 0 || ticket_amount < 0 || ticket_amount > 100)
		{
			fprintf(stderr, "%sdatacenter %d encountered an error while converting request message (errno: %d)", 
				err_msg, id, errno);
			thread_rets->ret = 1;
			break;
		}

		request_sig = 1;
		while(claimed_sig == 0);
		request_sig = 0;

		//send the client the number indicating whether the request message was accepted
		memset(msg_buf, 0, msg_buf_max);
		if(ticket_amount > *pool)
		{
			strcpy(msg_buf, "0");
			if(ticket_amount == 1)
			{
				fprintf(stdout, "%sdatacenter %d rejected a request for %d ticket from its client\n", 
					log_msg, id, ticket_amount);
			}
			else
			{
				fprintf(stdout, "%sdatacenter %d rejected a request for %d tickets from its client\n", 
					log_msg, id, ticket_amount);
			}
		}
		else
		{
			*requested = ticket_amount;
			strcpy(msg_buf, "1");
			if(ticket_amount == 1)
			{
				fprintf(stdout, "%sdatacenter %d accepted a request for %d ticket from its client\n", 
					log_msg, id, ticket_amount);
			}
			else
			{
				fprintf(stdout, "%sdatacenter %d accepted a request for %d tickets from its client\n", 
					log_msg, id, ticket_amount);
			}
		}

		while(release_sig == 0);
		*pool -= *requested;
		fprintf(stdout, "%squeue has been released\n", log_msg);
		delay(2);
		fprintf(stdout, "%sclient notified of sale completion\n", log_msg);
		status = send(client_rspd_sock_fd, msg_buf, msg_buf_max, 0);
		if(status != msg_buf_max)
		{
			fprintf(stderr, "%sdatacenter %d encoutnered an issue while sending response (status: %d/errno: %d)\n", 
				err_msg, id, status, errno);
			thread_rets->ret = 1;
			break;
		}



		//close the datacenter and client socket connections
		status = close(client_rspd_sock_fd);
		if(status < 0)
		{
			fprintf(stderr, "%sdatacenter %d failed to cleanly close the client_rspd socket (status: %d/errno: %d)\n", 
				err_msg, id, status, errno);
			thread_rets->ret = 1;
			break;
		}
		status = close(client_recv_sock_fd);
		if(status < 0)
		{
			fprintf(stderr, "%sdatacenter %d failed to cleanly close the client_recv socket (status: %d/errno: %d)\n", 
				err_msg, id, status, errno);
			thread_rets->ret = 1;
			break;
		}

		thread_rets->ret = 0;
		release_sig = 0;
	}

	fflush(stdout);
	return((void *)thread_rets);
}

//receive messages from other datacenters for synchronization
void *datacenter_recv_thread(void *args)
{
	int msg_buf_max = 4096;
	int sockopt = 1;
	int i;
	int j;
	int status;
	int id;
	int port;
	int count;
	int comm_flag;
	int datacenter_recv_sock_fd;
	int datacenter_rspd_sock_fd;
	int *pool;
	int *requested;
	int id_inc;
	int clock_inc;
	int msg_delay;
	char msg_buf[msg_buf_max];
	char *hostname;
	char *end;
	struct sockaddr_in datacenter_recv_addr;
	struct sockaddr_in datacenter_rspd_addr;
	socklen_t datacenter_recv_addr_len;
	socklen_t datacenter_rspd_addr_len;
	arg_obj *thread_args = (arg_obj *)args;
	ret_obj *thread_rets = (ret_obj *)malloc(sizeof(ret_obj));

	fflush(stdout);
	id = thread_args->id;
	pool = thread_args->pool;
	requested = thread_args->requested;
	msg_delay = thread_args->delay;
	count = thread_args->count;
	port = thread_args->port;
	hostname = thread_args->hostname;
	free(thread_args);

	while(1)
	{
		//setup datacenter_recv address object
		datacenter_recv_addr_len = sizeof(datacenter_recv_addr);
		memset((char *)&datacenter_recv_addr, 0, datacenter_recv_addr_len);
		datacenter_recv_addr.sin_family = AF_INET;
		datacenter_recv_addr.sin_addr.s_addr = inet_addr(hostname);
		datacenter_recv_addr.sin_port = htons(port);

		//open a datacenter_recv socket
		status = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
		if(status < 0)
		{
			fprintf(stderr, "%sdatacenter_recv %d failed to open a socket (status: %d/errno: %d)\n", 
				err_msg, id, status, errno);
			thread_rets->ret = 1;
			break;
		}
		datacenter_recv_sock_fd = status;

		//check if the system is still holding onto the port after a recent restart
		status = setsockopt(datacenter_recv_sock_fd, SOL_SOCKET, SO_REUSEADDR, (const void *)&sockopt , sizeof(int));
		if(status < 0)
		{
			fprintf(stderr, "%sdatacenter_recv %d can't bind to port due to the system still holding the port resources from a recent \
				restart (status: %d/errno: %d)\n", err_msg, id, status, errno);
			thread_rets->ret = 1;
			break;
		}

		//bind to a port on the datacenter
		status = bind(datacenter_recv_sock_fd, (struct sockaddr *)&datacenter_recv_addr, datacenter_recv_addr_len);
		if(status < 0)
		{
			fprintf(stderr, "%sdatacenter_recv %d failed to bind to the specified port (status: %d/errno: %d)\n", 
				err_msg, id, status, errno);
			thread_rets->ret = 1;
			break;
		}

		//listen to the port
		datacenter_rspd_addr_len = sizeof(datacenter_rspd_addr);
		status = listen(datacenter_recv_sock_fd, 16);
		if(status < 0)
		{
			fprintf(stderr, "%sdatacenter_recv %d failed to listen for clients (status: %d/errno: %d)\n", 
				err_msg, id, status, errno);
			thread_rets->ret = 1;
			break;
		}

		//accept connection from client
		status = accept(datacenter_recv_sock_fd, (struct sockaddr *)&datacenter_rspd_addr, &datacenter_rspd_addr_len);
		if(status < 0)
		{
			fprintf(stderr, "%sdatacenter_recv %d failed to accept a connection from a client (status: %d/errno: %d)\n", 
				err_msg, id, status, errno);
			thread_rets->ret = 1;
			break;
		}
		datacenter_rspd_sock_fd = status;

		//wait to receive a message from the client wiht the comm_flag
		memset(msg_buf, 0, msg_buf_max);
		status = recv(datacenter_rspd_sock_fd, msg_buf, msg_buf_max, 0);
		if(status != msg_buf_max)
		{
			fprintf(stderr, "%sdatacenter_recv %d encountered an issue while reading the message (status: %d/errno: %d)\n", 
				err_msg, id, status, errno);
			thread_rets->ret = 1;
			break;
		}

		//convert message to an int
		errno = 0;
		comm_flag = strtol(msg_buf, &end, 10);
		if(*end != 0 || errno != 0)
		{
			fprintf(stderr, "%sdatacenter_recv %d encountered an error while converting comm_flag message (errno: %d)", 
				err_msg, id, errno);
			thread_rets->ret = 1;
			break;
		}

		if(comm_flag == 0)
		{
			fprintf(stdout, "%sdatacenter_recv %d received a connection on comm 0\n", log_msg, id);

			//wait to receive a message from the client
			memset(msg_buf, 0, msg_buf_max);
			status = recv(datacenter_rspd_sock_fd, msg_buf, msg_buf_max, 0);
			if(status != msg_buf_max)
			{
				fprintf(stderr, "%sdatacenter_recv %d encountered an issue while reading the message (status: %d/errno: %d)\n", 
					err_msg, id, status, errno);
				thread_rets->ret = 1;
				break;
			}

			//convert message to an int
			errno = 0;
			id_inc = strtol(msg_buf, &end, 10);
			if(*end != 0 || errno != 0)
			{
				fprintf(stderr, "%sdatacenter_recv %d encountered an error while converting id_inc message (errno: %d)", 
					err_msg, id, errno);
				thread_rets->ret = 1;
				break;
			}

			//wait to receive a message from the client
			memset(msg_buf, 0, msg_buf_max);
			status = recv(datacenter_rspd_sock_fd, msg_buf, msg_buf_max, 0);
			if(status != msg_buf_max)
			{
				fprintf(stderr, "%sdatacenter_recv %d encountered an issue while reading the message (status: %d/errno: %d)\n", 
					err_msg, id, status, errno);
				thread_rets->ret = 1;
				break;
			}

			//convert message to an int
			errno = 0;
			clock_inc = strtol(msg_buf, &end, 10);
			if(*end != 0 || errno != 0)
			{
				fprintf(stderr, "%sdatacenter_recv %d encountered an error while converting clock_inc message (errno: %d)", 
					err_msg, id, errno);
				thread_rets->ret = 1;
				break;
			}

			//increment clock from the request
			if(clock_inc > l_clock)
				l_clock = clock_inc + 1;
			else
				l_clock += 1;

			for(i = 0; i < count+1; i++)
			{
				//schedule incoming after
				if(clock_queue[i] < l_clock)
				{
					continue;
				}
				//check process id
				else if(clock_queue[i] == l_clock)
				{
					//schedule incoming after
					if(id_queue[i] < id_inc)
					{
						continue;
					}
					//schedule incoming before
					else if(id_queue[i] > id_inc)
					{
						for(j = count; j > i; j--)
						{
							clock_queue[j] = clock_queue[j-1];
							id_queue[j] = id_queue[j-1];
						}
						clock_queue[j] = l_clock;
						id_queue[j] = id_inc;
						break;
					}
				}
				//schedule incoming before
				else if(clock_queue[i] > l_clock)
				{
					for(j = count; j > i; j--)
					{
						clock_queue[j] = clock_queue[j-1];
						id_queue[j] = id_queue[j-1];
					}
					clock_queue[j] = l_clock;
					id_queue[j] = id_inc;
					break;
				}
			}

			memset(msg_buf, 0, msg_buf_max);
			sprintf(msg_buf, "%d", id_queue[0]);
			delay(msg_delay);
			status = send(datacenter_rspd_sock_fd, msg_buf, msg_buf_max, 0);
			if(status != msg_buf_max)
			{
				fprintf(stderr, "%sdatacenter_recv %d encoutnered an issue while sending response (status: %d/errno: %d)\n", 
					err_msg, id, status, errno);
				thread_rets->ret = 1;
				break;
			}
			fprintf(stdout, "%sdatacenter_recv send %s\n", log_msg, msg_buf);

			memset(msg_buf, 0, msg_buf_max);
			sprintf(msg_buf, "%d", clock_queue[0]);
			delay(msg_delay);
			status = send(datacenter_rspd_sock_fd, msg_buf, msg_buf_max, 0);
			if(status != msg_buf_max)
			{
				fprintf(stderr, "%sdatacenter_recv %d encoutnered an issue while sending response (status: %d/errno: %d)\n", 
					err_msg, id, status, errno);
				thread_rets->ret = 1;
				break;
			}
			fprintf(stdout, "%sdatacenter_recv send %s\n", log_msg, msg_buf);
		}
		else
		{
			fprintf(stdout, "%sdatacenter_recv %d received a connection on comm 1\n", log_msg, id);

			//wait to receive a message from the client
			memset(msg_buf, 0, msg_buf_max);
			status = recv(datacenter_rspd_sock_fd, msg_buf, msg_buf_max, 0);
			if(status != msg_buf_max)
			{
				fprintf(stderr, "%sdatacenter_recv %d encountered an issue while reading the message (status: %d/errno: %d)\n", 
					err_msg, id, status, errno);
				thread_rets->ret = 1;
				break;
			}

			//convert message to an int
			errno = 0;
			*requested = strtol(msg_buf, &end, 10);
			if(*end != 0 || errno != 0 || *requested < 0 || *requested > 100)
			{
				fprintf(stderr, "%sdatacenter_recv %d encountered an error while converting request message (errno: %d)", 
					err_msg, id, errno);
				thread_rets->ret = 1;
				break;
			}

			//update queue and pool
			for(j = 0; j < count; j++)
			{
				clock_queue[j] = clock_queue[j+1];
				id_queue[j] = id_queue[j+1];
			}
			clock_queue[j] = 9;
			id_queue[j] = 9;
			*pool -= *requested;
			*requested = 0;
			fprintf(stdout, "%sthe pool has been updated to contain %d tickets\n", log_msg, *pool);
			l_clock += 1;
		}

		//close the datacenter and client socket connections
		status = close(datacenter_rspd_sock_fd);
		if(status < 0)
		{
			fprintf(stderr, "%sdatacenter_recv %d failed to cleanly close the client_rspd socket (status: %d/errno: %d)\n", 
				err_msg, id, status, errno);
			thread_rets->ret = 1;
			break;
		}
		status = close(datacenter_recv_sock_fd);
		if(status < 0)
		{
			fprintf(stderr, "%sdatacenter_recv %d failed to cleanly close the client_recv socket (status: %d/errno: %d)\n", 
				err_msg, id, status, errno);
			thread_rets->ret = 1;
			break;
		}

		thread_rets->ret = 0;
	}

	fflush(stdout);
	return((void *)thread_rets);
}

//send messages to all other datacenters for synchronization
void *datacenter_send_thread(void *args)
{
	int msg_buf_max = 4096;
	int retransmit = 0;
	int i;
	int status;
	int id;
	int port;
	int count;
	int datacenter_send_sock_fd;
	int *requested;
	int clock_queue_min = 1;
	int id_queue_min = 1;
	int comm_flag;
	int msg_delay;
	char msg_buf[msg_buf_max];
	char *end;
	struct sockaddr_in datacenter_send_addr;
	socklen_t datacenter_send_addr_len;
	datacenter_obj *datacenters;
	arg_obj *thread_args = (arg_obj *)args;
	ret_obj *thread_rets = (ret_obj *)malloc(sizeof(ret_obj));
	fflush(stdout);
	id = thread_args->id;
	requested = thread_args->requested;
	msg_delay = thread_args->delay;
	count = thread_args->count;
	port = thread_args->port;
	datacenters = thread_args->datacenters;
	free(thread_args);
	int clock_queue_res[count+1];
	int id_queue_res[count+1];

	while(1)
	{
		while(request_sig == 0);
		fprintf(stdout, "%sdatacenter_send %d request_sig has been triggered\n", log_msg, id);

		RETRANSMIT:
		for(i = 0; i < count; i++)
		{
			//skip yourself
			if(id == i+1)
			{
				continue;
			}
			fprintf(stdout, "%sdatacenter_send %d sending request message to datacenter #%d\n", log_msg, id, i+1);

			comm_flag = 0;
			datacenter_send_addr_len = sizeof(datacenter_send_addr);
			memset((char *)&datacenter_send_addr, 0, datacenter_send_addr_len);
			datacenter_send_addr.sin_family = AF_INET;
			datacenter_send_addr.sin_addr.s_addr = inet_addr(datacenters[i].hostname);
			datacenter_send_addr.sin_port = htons(port);

			//open a datacenter_send socket
			status = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
			if(status < 0)
			{
				fprintf(stderr, "%sdatacenter_send failed to open a socket (status: %d/errno: %d)\n", err_msg, status, errno);
				thread_rets->ret = 1;
				break;
			}
			datacenter_send_sock_fd = status;

			//connect to datacenter
			status = connect(datacenter_send_sock_fd, (struct sockaddr*)&datacenter_send_addr, datacenter_send_addr_len);
			if(status < 0)
			{
				fprintf(stdout, "%sdatacenter_send skipping datacenter %d since it's not online\n", log_msg, i+1);
				continue;
			}

			//send the datacenter the process id
			RETRY_COMMA_SEND:
			sprintf(msg_buf, "%d", comm_flag);
			delay(msg_delay);
			status = send(datacenter_send_sock_fd, msg_buf, msg_buf_max, 0);
			if(status == -1 && errno == 104)
			{
				//close the socket connection to the datacenter and try again
				fprintf(stdout, "%sdatacenter_send was interrupted while sending comma, retrying...\n", log_msg);
				status = close(datacenter_send_sock_fd);
				if(status < 0)
				{
					fprintf(stderr, "%sdatacenter_send failed to cleanly close the socket (status: %d/errno: %d)\n", 
						err_msg, status, errno);
					thread_rets->ret = 1;
					break;
				}
				//open a datacenter_send socket
				status = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
				if(status < 0)
				{
					fprintf(stderr, "%sdatacenter_send failed to open a socket (status: %d/errno: %d)\n", err_msg, status, errno);
					thread_rets->ret = 1;
					break;
				}
				datacenter_send_sock_fd = status;
				//connect to datacenter
				status = connect(datacenter_send_sock_fd, (struct sockaddr*)&datacenter_send_addr, datacenter_send_addr_len);
				if(status < 0)
				{
					fprintf(stdout, "%sdatacenter_send skipping datacenter %d since it's not online\n", log_msg, i+1);
					continue;
				}
				goto RETRY_COMMA_SEND;
			}
			else if(status != msg_buf_max)
			{
				fprintf(stderr, "%sdatacenter_send encoutnered an issue while sending request (status: %d/errno: %d)\n", 
					err_msg, status, errno);
				thread_rets->ret = 1;
				break;
			}

			//send the datacenter the process id
			RETRY_ID_SEND:
			sprintf(msg_buf, "%d", id);
			delay(msg_delay);
			status = send(datacenter_send_sock_fd, msg_buf, msg_buf_max, 0);
			if(status == -1 && errno == 104)
			{
				//close the socket connection to the datacenter and try again
				fprintf(stdout, "%sdatacenter_send was interrupted while sending id, retrying...\n", log_msg);
				status = close(datacenter_send_sock_fd);
				if(status < 0)
				{
					fprintf(stderr, "%sdatacenter_send failed to cleanly close the socket (status: %d/errno: %d)\n", 
						err_msg, status, errno);
					thread_rets->ret = 1;
					break;
				}
				//open a datacenter_send socket
				status = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
				if(status < 0)
				{
					fprintf(stderr, "%sdatacenter_send failed to open a socket (status: %d/errno: %d)\n", err_msg, status, errno);
					thread_rets->ret = 1;
					break;
				}
				datacenter_send_sock_fd = status;
				//connect to datacenter
				status = connect(datacenter_send_sock_fd, (struct sockaddr*)&datacenter_send_addr, datacenter_send_addr_len);
				if(status < 0)
				{
					fprintf(stdout, "%sdatacenter_send skipping datacenter %d since it's not online\n", log_msg, i+1);
					continue;
				}
				goto RETRY_ID_SEND;
			}
			else if(status != msg_buf_max)
			{
				fprintf(stderr, "%sdatacenter_send encoutnered an issue while sending request (status: %d/errno: %d)\n", 
					err_msg, status, errno);
				thread_rets->ret = 1;
				break;
			}

			//send the datacenter the clock
			RETRY_CLOCK_SEND:
			sprintf(msg_buf, "%d", l_clock);
			delay(msg_delay);
			status = send(datacenter_send_sock_fd, msg_buf, msg_buf_max, 0);
			if(status == -1 && errno == 104)
			{
				//close the socket connection to the datacenter and try again
				fprintf(stdout, "%sdatacenter_send was interrupted while sending clock, retrying...\n", log_msg);
				status = close(datacenter_send_sock_fd);
				if(status < 0)
				{
					fprintf(stderr, "%sdatacenter_send failed to cleanly close the socket (status: %d/errno: %d)\n", 
						err_msg, status, errno);
					thread_rets->ret = 1;
					break;
				}
				//open a datacenter_send socket
				status = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
				if(status < 0)
				{
					fprintf(stderr, "%sdatacenter_send failed to open a socket (status: %d/errno: %d)\n", err_msg, status, errno);
					thread_rets->ret = 1;
					break;
				}
				datacenter_send_sock_fd = status;
				//connect to datacenter
				status = connect(datacenter_send_sock_fd, (struct sockaddr*)&datacenter_send_addr, datacenter_send_addr_len);
				if(status < 0)
				{
					fprintf(stdout, "%sdatacenter_send skipping datacenter %d since it's not online\n", log_msg, i+1);
					continue;
				}
				goto RETRY_CLOCK_SEND;
			}
			else if(status != msg_buf_max)
			{
				fprintf(stderr, "%sdatacenter_send encoutnered an issue while sending request (status: %d/errno: %d)\n", 
					err_msg, status, errno);
				thread_rets->ret = 1;
				break;
			}
	
			//wait to receive a response from the datacenter
			RETRY_ID_RECV:
			status = recv(datacenter_send_sock_fd, msg_buf, msg_buf_max, 0);
			if(status == -1 && errno == 104)
			{
				//close the socket connection to the datacenter and try again
				fprintf(stdout, "%sdatacenter_send was interrupted while receving id, retrying...\n", log_msg);
				status = close(datacenter_send_sock_fd);
				if(status < 0)
				{
					fprintf(stderr, "%sdatacenter_send failed to cleanly close the socket (status: %d/errno: %d)\n", 
						err_msg, status, errno);
					thread_rets->ret = 1;
					break;
				}
				//open a datacenter_send socket
				status = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
				if(status < 0)
				{
					fprintf(stderr, "%sdatacenter_send failed to open a socket (status: %d/errno: %d)\n", err_msg, status, errno);
					thread_rets->ret = 1;
					break;
				}
				datacenter_send_sock_fd = status;
				//connect to datacenter
				status = connect(datacenter_send_sock_fd, (struct sockaddr*)&datacenter_send_addr, datacenter_send_addr_len);
				if(status < 0)
				{
					fprintf(stdout, "%sdatacenter_send skipping datacenter %d since it's not online\n", log_msg, i+1);
					continue;
				}
				goto RETRY_ID_RECV;
			}
			else if(status != msg_buf_max)
			{
				fprintf(stderr, "%sdatacenter_send encountered an issue while reading the response (status: %d/errno: %d)\n", 
					err_msg, status, errno);
				thread_rets->ret = 1;
				break;
			}

			//convert response to an int
			errno = 0;
			id_queue_res[i] = strtol(msg_buf, &end, 10);
			if(*end != 0 || errno != 0)
			{
				fprintf(stderr, "%sdatacenter_send encountered an error while converting response id (errno: %d)", 
					err_msg, errno);
				thread_rets->ret = 1;
				break;
			}

			//wait to receive a response from the datacenter
			RETRY_CLOCK_RECV:
			status = recv(datacenter_send_sock_fd, msg_buf, msg_buf_max, 0);
			if(status == -1 && errno == 104)
			{
				//close the socket connection to the datacenter and try again
				fprintf(stdout, "%sdatacenter_send was interrupted while receiving clock, retrying...\n", log_msg);
				status = close(datacenter_send_sock_fd);
				if(status < 0)
				{
					fprintf(stderr, "%sdatacenter_send failed to cleanly close the socket (status: %d/errno: %d)\n", 
						err_msg, status, errno);
					thread_rets->ret = 1;
					break;
				}
				//open a datacenter_send socket
				status = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
				if(status < 0)
				{
					fprintf(stderr, "%sdatacenter_send failed to open a socket (status: %d/errno: %d)\n", err_msg, status, errno);
					thread_rets->ret = 1;
					break;
				}
				datacenter_send_sock_fd = status;
				//connect to datacenter
				status = connect(datacenter_send_sock_fd, (struct sockaddr*)&datacenter_send_addr, datacenter_send_addr_len);
				if(status < 0)
				{
					fprintf(stdout, "%sdatacenter_send skipping datacenter %d since it's not online\n", log_msg, i+1);
					continue;
				}
				goto RETRY_CLOCK_RECV;
			}
			else if(status != msg_buf_max)
			{
				fprintf(stderr, "%sdatacenter_send encountered an issue while reading the response (status: %d/errno: %d)\n", 
					err_msg, status, errno);
				thread_rets->ret = 1;
				break;
			}

			//convert response to an int
			errno = 0;
			clock_queue_res[i] = strtol(msg_buf, &end, 10);
			if(*end != 0 || errno != 0)
			{
				fprintf(stderr, "%sdatacenter_send encountered an error while converting response clock (errno: %d)", 
					err_msg, errno);
				thread_rets->ret = 1;
				break;
			}

			//close the socket connection to the datacenter
			status = close(datacenter_send_sock_fd);
			if(status < 0)
			{
				fprintf(stderr, "%sdatacenter_send failed to cleanly close the socket (status: %d/errno: %d)\n", 
					err_msg, status, errno);
				thread_rets->ret = 1;
				break;
			}

			if(clock_queue_res[i] > clock_queue_min)
			{
				clock_queue_min = clock_queue_res[i];
				id_queue_min = id_queue_res[i];
			}
		}
		if(thread_rets->ret == 1)
		{
			break;
		}
		//increment clock from the request
		if(retransmit == 0)
		{
			if(clock_queue_min > l_clock)
				l_clock = clock_queue_min + 1;
			else
				l_clock += 1;
		}

		//check if our process claimed priority over the pool
		fprintf(stdout, "%sdatacenter_send has clock: %d/id: %d, queue_response has clock: %d/id: %d\n", 
			log_msg, l_clock, id, clock_queue_min, id_queue_min);
		if(id_queue_min == id)
		{
			claimed_sig = 1;
		}
		else
		{
			//retransmit the request
			fprintf(stdout, "%sdatacenter_send waiting for hold claim on queue, retransmitting...\n", log_msg);
			retransmit = 1;
			memset(clock_queue_res, 0, sizeof(clock_queue_res));
			memset(id_queue_res, 0, sizeof(id_queue_res));
			goto RETRANSMIT;
		}

		thread_rets->ret = 0;
		while(claimed_sig == 0);
		retransmit = 0;
		fprintf(stdout, "%sdatacenter_send claimed_sig has been triggered\n", log_msg);

		for(i = 0; i < count; i++)
		{
			//skip yourself
			if(id == i+1)
			{
				continue;
			}
			fprintf(stdout, "%sdatacenter_send sending release message to datacenter #%d\n", log_msg, i+1);

			comm_flag = 1;
			datacenter_send_addr_len = sizeof(datacenter_send_addr);
			memset((char *)&datacenter_send_addr, 0, datacenter_send_addr_len);
			datacenter_send_addr.sin_family = AF_INET;
			datacenter_send_addr.sin_addr.s_addr = inet_addr(datacenters[i].hostname);
			datacenter_send_addr.sin_port = htons(port);

			//open a datacenter_send socket
			status = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
			if(status < 0)
			{
				fprintf(stderr, "%sdatacenter_send failed to open a socket (status: %d/errno: %d)\n", err_msg, status, errno);
				thread_rets->ret = 1;
				break;
			}
			datacenter_send_sock_fd = status;

			//connect to datacenter
			status = connect(datacenter_send_sock_fd, (struct sockaddr*)&datacenter_send_addr, datacenter_send_addr_len);
			if(status < 0)
			{
				fprintf(stdout, "%sdatacenter_send skipping datacenter %d since it's not online\n", log_msg, i+1);
				continue;
			}

			//send the datacenter the process id
			RETRY_COMMB_SEND:
			sprintf(msg_buf, "%d", comm_flag);
			delay(msg_delay);
			status = send(datacenter_send_sock_fd, msg_buf, msg_buf_max, 0);
			if(status == -1 && errno == 104)
			{
				//close the socket connection to the datacenter and try again
				fprintf(stdout, "%sdatacenter_send was interrupted while sending commb, retrying...\n", log_msg);
				status = close(datacenter_send_sock_fd);
				if(status < 0)
				{
					fprintf(stderr, "%sdatacenter_send failed to cleanly close the socket (status: %d/errno: %d)\n", 
						err_msg, status, errno);
					thread_rets->ret = 1;
					break;
				}
				//open a datacenter_send socket
				status = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
				if(status < 0)
				{
					fprintf(stderr, "%sdatacenter_send failed to open a socket (status: %d/errno: %d)\n", err_msg, status, errno);
					thread_rets->ret = 1;
					break;
				}
				datacenter_send_sock_fd = status;
				//connect to datacenter
				status = connect(datacenter_send_sock_fd, (struct sockaddr*)&datacenter_send_addr, datacenter_send_addr_len);
				if(status < 0)
				{
					fprintf(stdout, "%sdatacenter_send skipping datacenter %d since it's not online\n", log_msg, i+1);
					continue;
				}
				goto RETRY_COMMB_SEND;
			}
			else if(status != msg_buf_max)
			{
				fprintf(stderr, "%sdatacenter_send encoutnered an issue while sending request (status: %d/errno: %d)\n", 
					err_msg, status, errno);
				thread_rets->ret = 1;
				break;
			}
	
			//send the datacenter the amount that was sold
			RETRY_TICKETR_SEND:
			sprintf(msg_buf, "%d", *requested);
			delay(msg_delay);
			status = send(datacenter_send_sock_fd, msg_buf, msg_buf_max, 0);
			if(status == -1 && errno == 104)
			{
				//close the socket connection to the datacenter and try again
				fprintf(stdout, "%sdatacenter_send was interrupted while sending requested amount for release, retrying...\n", 
					log_msg);
				status = close(datacenter_send_sock_fd);
				if(status < 0)
				{
					fprintf(stderr, "%sdatacenter_send failed to cleanly close the socket (status: %d/errno: %d)\n", 
						err_msg, status, errno);
					thread_rets->ret = 1;
					break;
				}
				//open a datacenter_send socket
				status = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
				if(status < 0)
				{
					fprintf(stderr, "%sdatacenter_send failed to open a socket (status: %d/errno: %d)\n", err_msg, status, errno);
					thread_rets->ret = 1;
					break;
				}
				datacenter_send_sock_fd = status;
				//connect to datacenter
				status = connect(datacenter_send_sock_fd, (struct sockaddr*)&datacenter_send_addr, datacenter_send_addr_len);
				if(status < 0)
				{
					fprintf(stdout, "%sdatacenter_send skipping datacenter %d since it's not online\n", log_msg, i+1);
					continue;
				}
				goto RETRY_TICKETR_SEND;
			}
			else if(status != msg_buf_max)
			{
				fprintf(stderr, "%sdatacenter_send encoutnered an issue while sending request (status: %d/errno: %d)\n", 
					err_msg, status, errno);
				thread_rets->ret = 1;
				break;
			}

			//close the socket connection to the datacenter
			status = close(datacenter_send_sock_fd);
			if(status < 0)
			{
				fprintf(stderr, "%sdatacenter_send failed to cleanly close the socket (status: %d/errno: %d)\n", 
					err_msg, status, errno);
				thread_rets->ret = 1;
				break;
			}

			l_clock += 1;
		}
		release_sig = 1;
		if(thread_rets->ret == 1)
		{
			break;
		}
		claimed_sig = 0;

		thread_rets->ret = 0;
	}

	fflush(stdout);
	return((void *)thread_rets);
}

//Read from stdin to catch ctrl^c/ctrl^d
void *stdin_thread(void *args)
{
	int stdin_buf_max = 4096;
	char *exit = "q\n";
	char stdin_buf[stdin_buf_max];
	arg_obj *thread_args = (arg_obj *)args;
	ret_obj *thread_rets = (ret_obj *)malloc(sizeof(ret_obj));

	fflush(stdout);
	free(thread_args);

	while(1)
	{
		if(terminate_sig == 1)
		{
			thread_rets->ret = 0;
			break;
		}

		//flush stdout and reset the stdin_buf memory
		fflush(stdout);
		memset(stdin_buf, 0, stdin_buf_max);

		//if crtl^c or crtl^d was received from stdin
		if(!fgets(stdin_buf, stdin_buf_max, stdin))
		{
			thread_rets->ret = 0;
			terminate_sig = 1;
			break;
		}
		if(strcmp(stdin_buf, exit) == 0)
		{
			thread_rets->ret = 0;
			terminate_sig = 1;
			break;
		}
	}

	fflush(stdout);
	return ((void *)thread_rets);
}



