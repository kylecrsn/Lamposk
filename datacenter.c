#include "datacenter.h"

int datacenter_handler()
{
/*	request_sig = 0;
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
	int datacenter_recv_port;*/
/*	int client_recv_ret_val;
	int datacenter_recv_ret_val;
	int datacenter_send_ret_val;*/
	// const char *hostname;
/*	config_setting_t *datacenter_addr_settings;
	config_setting_t *datacenter_addr_elem_setting;
	config_setting_t *datacenter_addr_running_setting;
	datacenter_obj datacenter;
	datacenter_obj *datacenters;*/

/*	ret_obj *client_recv_rets;
	ret_obj *datacenter_recv_rets;
	ret_obj *datacenter_send_rets;*/


	/*
		REVISED VARIABLES
	*/
	char *fnc_m = "datacenter_handler";
	const char *hn;
	int32_t i_dc = -1;
	int32_t i;
	int32_t status;
	int32_t dc_addr_count;
	int32_t dc_addr_id;
	int32_t ticket_pool;
	int32_t ticket_pool_init;
	int32_t msg_delay;
	int32_t cl_lstn_port;
	int32_t dc_lstn_port_base;
	config_t cf_local;
	config_t *cf;
	struct flock *fl;
	int32_t fd;
	time_t boot_time;
	config_setting_t *dc_addr_settings;
	config_setting_t *dc_addr_elem_setting;
	dc_obj *dc_sys;
	pthread_t cl_lstn_thread_id;
	// pthread_t dc_bcst_thread_id;
	// pthread_t dc_lstn_thread_id;


	arg_obj *cl_lstn_args;
	// arg_obj *dc_bcst_args;
	// arg_obj *dc_lstn_args;


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
		fprintf(stderr, "%s%d%s (%s) encountered an issue while reading from config file: %s:%d - %s\n", 
			err_m, 0, cls_m, fnc_m, config_error_file(cf), config_error_line(cf), config_error_text(cf));
		config_destroy(cf);
		unlock_cfg(fd, fl);
		close(fd);
		return 1;
	}

	//get general config values
	config_lookup_int(cf, "delay", &msg_delay);
	config_lookup_int(cf, "dc.ticket_pool_init", &ticket_pool_init);
	config_lookup_int(cf, "dc.ticket_pool", &ticket_pool);
	config_lookup_int(cf, "dc.cl_lstn_port", &cl_lstn_port);
	config_lookup_int(cf, "dc.dc_lstn_port_base", &dc_lstn_port_base);
	dc_addr_settings = config_lookup(cf, "dc.addrs");
	dc_addr_count = config_setting_length(dc_addr_settings);

	//populate an array with all of the datacenter configs
	dc_sys = (dc_obj *)malloc(dc_addr_count * sizeof(dc_obj));
	for(i = 0; i < dc_addr_count; i++)
	{
		dc_addr_elem_setting = config_setting_get_elem(dc_addr_settings, i);
		config_setting_lookup_int(dc_addr_elem_setting, "id", &(dc_sys[i].id));
		config_setting_lookup_int(dc_addr_elem_setting, "online", &(dc_sys[i].online));
		config_setting_lookup_string(dc_addr_elem_setting, "hostname", &hn);
		dc_sys[i].clk = 0;
		dc_sys[i].hostname = (char *)malloc(strlen(hn) + 1);
		memcpy(dc_sys[i].hostname, hn, strlen(hn) + 1);

		//set the index for the first availble datacenter not currently online
		if(dc_sys[i].online == 0 && i_dc == -1)
		{
			i_dc = i;

			//set the online status to 1
			config_setting_set_int(config_setting_get_member(dc_addr_elem_setting, "online"), 1);
			dc_sys[i].online = 1;
			config_write_file(cf, "global.cfg");
		}
	}

	//all datacenters are already online
	if(dc_sys[i-1].online == 1)
	{
		fprintf(stderr, "%s%d%s (%s) can't start another datacenter, all datacenters specified by global.cfg are currently online\n", 
			err_m, 0, cls_m, fnc_m);
		config_destroy(cf);
		unlock_cfg(fd, fl);
		close(fd);
		return 1;
	}

	//Close and unlock the config file
	config_destroy(cf);
	unlock_cfg(fd, fl);
	close(fd);

	//log datacenter metadata
	time(&boot_time);
	fprintf(stdout, "========================\n|   Datacenter #%d Log   |\n========================\n\n", dc_sys[i_dc].id);
	fprintf(stdout, "- Boot Time: %s\n", asctime(localtime(&boot_time)));
	fprintf(stdout, "- ID: %d\n", dc_sys[i_dc].id);
	fprintf(stdout, "- Initial Clock: %d\n", dc_sys[i_dc].clk);
	fprintf(stdout, "- Initial Ticket Pool: %d\n", ticket_pool_init);
	fprintf(stdout, "- Current Ticket Pool: %d\n", ticket_pool);
	fprintf(stdout, "- Client Listen Port: %d\n", cl_lstn_port);
	fprintf(stdout, "- Datacenter Broadcast Base Port: %d\n",dc_lstn_port_base);
	fprintf(stdout, "- Datacenter Hostname: %s\n\n\n", dc_sys[i_dc].hostname);

	//spawn cl_lstn_thread
	cl_lstn_args = (arg_obj *)malloc(sizeof(arg_obj));
	cl_lstn_args->port = cl_lstn_port;
	cl_lstn_args->hostname = dc_sys[i_dc].hostname;
	fflush(stdout);
	status = pthread_create(&cl_lstn_thread_id, NULL, cl_lstn_thread, (void *)cl_lstn_args);
	if(status != 0)
	{
		fprintf(stderr, "%s%d%s (%s) failed to spawn cl_lstn_thread (errno: %s)\n", 
			err_m, dc_sys[i_dc].clk, cls_m, fnc_m, strerror(errno));
		return 1;
	}
/*
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
			err_m, datacenter.id, status, strerror(errno));
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
			err_m, datacenter.id, status, strerror(errno));
		return 1;
	}*/


	fprintf(stdout, "%s%d%s (%s) finished spawning primary child threads\n", 
			log_m, dc_sys[i_dc].clk, cls_m, fnc_m);

	//JOIN THREADS

	fprintf(stdout, "%s%d%s (%s) finished joining primary child threads\n", 
			log_m, dc_sys[i_dc].clk, cls_m, fnc_m);

	//free the dynamic dc_obj memory
	for(i = 0; i < dc_addr_count; i++)
	{
		free(dc_sys[i].hostname);
	}
	free(dc_sys);

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
		fprintf(stderr, "%s%d%s (%s) encountered an issue while reading from config file: %s:%d - %s\n", 
			err_m, 0, cls_m, fnc_m, config_error_file(cf), config_error_line(cf), config_error_text(cf));
		config_destroy(cf);
		unlock_cfg(fd, fl);
		close(fd);
		return 1;
	}

	//set the online status back to 0
	dc_addr_settings = config_lookup(cf, "dc.addrs");
	for(i = 0; i < dc_addr_count; i++)
	{
		dc_addr_elem_setting = config_setting_get_elem(dc_addr_settings, i);
		config_setting_lookup_int(dc_addr_elem_setting, "id", &dc_addr_id);
		if(dc_addr_id == i_dc+1)
		{
			break;
		}
	}
	config_setting_set_int(config_setting_get_member(dc_addr_elem_setting, "online"), 0);
	config_write_file(cf, "global.cfg");

	//Close and unlock the config file
	config_destroy(cf);
	unlock_cfg(fd, fl);
	close(fd);

	return 0;
}

//acts as a frontend for communicating with the client
void *cl_lstn_thread(void *args)
{
	int32_t msg_buf_max = 4096;
	int32_t sockopt = 1;
	int32_t status;
	int32_t cl_lstn_sock_fd;
	int32_t cl_rspd_sock_fd;
	uint32_t port;
	uint32_t *pool;
	uint32_t *requested;
	uint32_t ticket_amount;
	char msg_buf[msg_buf_max];
	char *fnc_m = "cl_lstn_thread";
	char *hostname;
	char *end;
	struct sockaddr_in cl_lstn_addr;
	struct sockaddr_in cl_rspd_addr;
	socklen_t cl_lstn_addr_len;
	socklen_t cl_rspd_addr_len;
	arg_obj *thread_args = (arg_obj *)args;
	ret_obj *thread_rets = (ret_obj *)malloc(sizeof(ret_obj));

	//read out data packed in the args parameter
	fflush(stdout);
	fflush(stderr);
	port = thread_args->port;
	hostname = thread_args->hostname;
	free(thread_args);
	thread_rets->ret = 0;

	//setup the cl_lstn address object
	cl_lstn_addr_len = sizeof(cl_lstn_addr);
	memset((char *)&cl_lstn_addr, 0, cl_lstn_addr_len);
	cl_lstn_addr.sin_family = AF_INET;
	cl_lstn_addr.sin_addr.s_addr = inet_addr(hostname);
	cl_lstn_addr.sin_port = htons(port);

	//open a server socket in the datacenter to accept incoming client requests
	status = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
	if(status < 0)
	{
		fprintf(stderr, "%s%d%s (%s) failed to open a socket (errno: %s)\n", 
			err_m, dc->clk, cls_m, fnc_m, strerror(errno));
		thread_rets->ret = 1;
	}
	cl_lstn_sock_fd = status;
	fprintf(stdout, "%s%d%s (%s) finished initializing a socket for client connections\n", 
			log_m, dc->clk, cls_m, fnc_m);

	//check if the system is still holding onto the port after a recent restart
	status = setsockopt(cl_lstn_sock_fd, SOL_SOCKET, SO_REUSEADDR, (const void *)&sockopt, sizeof(int));
	if(status < 0)
	{
		fprintf(stderr, "%s%d%s (%s) can't bind to port due to a recent program restart (errno: %s)\n", 
			err_m, dc->clk, cls_m, fnc_m, strerror(errno));
		thread_rets->ret = 1;
	}
	fprintf(stdout, "%s%d%s (%s) finished setting the socket to be reusable\n", 
			log_m, dc->clk, cls_m, fnc_m);

	//bind to a port on the datacenter
	status = bind(cl_lstn_sock_fd, (struct sockaddr *)&cl_lstn_addr, cl_lstn_addr_len);
	if(status < 0)
	{
		fprintf(stderr, "%s%d%s (%s) failed to bind to the specified port (errno: %s)\n", 
			err_m, dc->clk, cls_m, fnc_m, strerror(errno));
		thread_rets->ret = 1;
	}
	fprintf(stdout, "%s%d%s (%s) finished binding to the socket\n", 
			log_m, dc->clk, cls_m, fnc_m);

	//listen to the port for an incoming client connection
	status = listen(cl_lstn_sock_fd, 1);
	if(status < 0)
	{
		fprintf(stderr, "%s%d%s (%s) failed to listen to the port for clients (errno: %s)\n", 
			err_m, dc->clk, cls_m, fnc_m, strerror(errno));
		thread_rets->ret = 1;
	}
	cl_rspd_addr_len = sizeof(cl_rspd_addr);
	fprintf(stdout, "%s%d%s (%s) now listening for client connections\n", 
			log_m, dc->clk, cls_m, fnc_m);

	//continuosly accept new client connections until an error occurs or the thread is ended
	while(1)
	{
		fprintf(stdout, "%s%d%s (%s) ticket pool amount before accepting a new client: %d\n", 
			log_m, dc->clk, cls_m, fnc_m, *pool);

		//accept connection from client
		status = accept(cl_lstn_sock_fd, (struct sockaddr *)&cl_rspd_addr, &ccl_rspd_addr_len);
		if(status < 0)
		{
			fprintf(stderr, "%s%d%s (%s) failed to accept a connection from a client (errno: %s)\n", 
				err_m, dc->clk, cls_m, fnc_m, strerror(errno));
			thread_rets->ret = 1;
			break;
		}
		cl_rspd_sock_fd = status;
		fprintf(stdout, "%s%d%s (%s) accepted a new client connection\n", 
			log_m, dc->clk, cls_m, fnc_m);

		//wait to receive a message from the client
		memset(msg_buf, 0, msg_buf_max);
		status = recv(cl_rspd_sock_fd, msg_buf, msg_buf_max, 0);
		if(status != msg_buf_max)
		{
			fprintf(stderr, "%s%d%s (%s) encountered an issue while reading the message (status: %d/errno: %d)\n", 
				err_m, dc->clk, cls_m, fnc_m, status, strerror(errno));
			thread_rets->ret = 1;
			break;
		}
		fprintf(stdout, "%s%d%s (%s) received a request from a client\n", 
			log_m, dc->clk, cls_m, fnc_m);

		//convert message to an int
		errno = 0;
		ticket_amount = strtol(msg_buf, &end, 10);
		if(*end != 0 || errno != 0)
		{
			fprintf(stderr, "%s%d%s (%s) encountered an error while converting client ticket request message (errno: %s)\n", 
				err_m, dc->clk, cls_m, fnc_m, strerror(errno));
			thread_rets->ret = 1;
			break;
		}

		//WAIT FOR THREADS TO NEGOTIATE ACCESS TO TICKET POOL

		fprintf(stdout, "%s%d%s (%s) ticket pool control has been obtained, processing request\n", 
			log_m, dc->clk, cls_m, fnc_m);

		//set a flag indicating whether the request message was accepted
		memset(msg_buf, 0, msg_buf_max);
		if(ticket_amount > *pool)
		{
			strcpy(msg_buf, "0");
			fprintf(stdout, "%s%d%s (%s) rejected a request from a client for %d ", ticket_amount);
			print_tickets(ticket_amount);
			fprintf(stdout, " (%d ", *pool);
			print_tickets(*pool);
			fprintf(stdout, " remaining in the pool)\n");
			fflush(stdout);
		}
		else
		{
			*requested = ticket_amount;
			*pool -= *requested;
			strcpy(msg_buf, "1");
			fprintf(stdout, "%s%d%s (%s) accepted a request from a client for %d ", ticket_amount);
			print_tickets(ticket_amount);
			fprintf(stdout, " (%d ", *pool);
			print_tickets(*pool);
			fprintf(stdout, " remaining in the pool)\n");
			fflush(stdout);
		}

		fprintf(stdout, "%s%d%s (%s) request completed, allowing release of ticket pool control\n", 
			log_m, dc->clk, cls_m, fnc_m);

		//ALLOW THREADS TO RELEASE ACCESS OF TICKET POOL

		//send the client the request results
		status = send(cl_rspd_sock_fd, msg_buf, msg_buf_max, 0);
		if(status != msg_buf_max)
		{
			fprintf(stderr, "%s%d%s (%s) encountered an issue while sending the message (status: %d/errno: %s)\n", 
				err_m, dc->clk, cls_m, fnc_m, status, strerror(errno));
			thread_rets->ret = 1;
			break;
		}
		fprintf(stdout, "%s%d%s (%s) sent the request results back to the client\n", 
			log_m, dc->clk, cls_m, fnc_m);

		//close the response socket for the client
		status = close(cl_rspd_sock_fd);
		if(status < 0)
		{
			fprintf(stderr, "%s%d%s (%s) failed to cleanly close the client response socket (errno: %s)\n", 
				err_m, dc->clk, cls_m, fnc_m, strerror(errno));
			thread_rets->ret = 1;
			break;
		}
		fprintf(stdout, "%s%d%s (%s) closed the response point of the client connection\n", 
			log_m, dc->clk, cls_m, fnc_m);

		thread_rets->ret = 0;
	}

	//close the datacenter's socket for incoming client requests
	status = close(cl_lstn_sock_fd);
	if(status < 0)
	{
			fprintf(stderr, "%s%d%s (%s) failed to cleanly close the datacenter's socket for client requests (errno: %s)\n", 
				err_m, dc->clk, cls_m, fnc_m, strerror(errno));
		thread_rets->ret = 1;
		break;
	}
	fprintf(stdout, "%s%d%s (%s) closed the datacenter's socket for client requests\n", 
			log_m, dc->clk, cls_m, fnc_m);

	fflush(stdout);
	fflush(stderr);
	pthread_exit((void *)thread_rets);
}

/*//receive messages from other datacenters for synchronization
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
				err_m, id, status, strerror(errno));
			thread_rets->ret = 1;
			break;
		}
		datacenter_recv_sock_fd = status;

		//check if the system is still holding onto the port after a recent restart
		status = setsockopt(datacenter_recv_sock_fd, SOL_SOCKET, SO_REUSEADDR, (const void *)&sockopt , sizeof(int));
		if(status < 0)
		{
			fprintf(stderr, "%sdatacenter_recv %d can't bind to port due to the system still holding the port resources from a recent \
				restart (status: %d/errno: %d)\n", err_m, id, status, strerror(errno));
			thread_rets->ret = 1;
			break;
		}

		//bind to a port on the datacenter
		status = bind(datacenter_recv_sock_fd, (struct sockaddr *)&datacenter_recv_addr, datacenter_recv_addr_len);
		if(status < 0)
		{
			fprintf(stderr, "%sdatacenter_recv %d failed to bind to the specified port (status: %d/errno: %d)\n", 
				err_m, id, status, strerror(errno));
			thread_rets->ret = 1;
			break;
		}

		//listen to the port
		datacenter_rspd_addr_len = sizeof(datacenter_rspd_addr);
		status = listen(datacenter_recv_sock_fd, 16);
		if(status < 0)
		{
			fprintf(stderr, "%sdatacenter_recv %d failed to listen for clients (status: %d/errno: %d)\n", 
				err_m, id, status, strerror(errno));
			thread_rets->ret = 1;
			break;
		}

		//accept connection from client
		status = accept(datacenter_recv_sock_fd, (struct sockaddr *)&datacenter_rspd_addr, &datacenter_rspd_addr_len);
		if(status < 0)
		{
			fprintf(stderr, "%sdatacenter_recv %d failed to accept a connection from a client (status: %d/errno: %d)\n", 
				err_m, id, status, strerror(errno));
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
				err_m, id, status, strerror(errno));
			thread_rets->ret = 1;
			break;
		}

		//convert message to an int
		errno = 0;
		comm_flag = strtol(msg_buf, &end, 10);
		if(*end != 0 || errno != 0)
		{
			fprintf(stderr, "%sdatacenter_recv %d encountered an error while converting comm_flag message (errno: %s)", 
				err_m, id, strerror(errno));
			thread_rets->ret = 1;
			break;
		}

		if(comm_flag == 0)
		{
			fprintf(stdout, "%sdatacenter_recv %d received a connection on comm 0\n", log_m, id);

			//wait to receive a message from the client
			memset(msg_buf, 0, msg_buf_max);
			status = recv(datacenter_rspd_sock_fd, msg_buf, msg_buf_max, 0);
			if(status != msg_buf_max)
			{
				fprintf(stderr, "%sdatacenter_recv %d encountered an issue while reading the message (status: %d/errno: %d)\n", 
					err_m, id, status, strerror(errno));
				thread_rets->ret = 1;
				break;
			}

			//convert message to an int
			errno = 0;
			id_inc = strtol(msg_buf, &end, 10);
			if(*end != 0 || errno != 0)
			{
				fprintf(stderr, "%sdatacenter_recv %d encountered an error while converting id_inc message (errno: %s)", 
					err_m, id, strerror(errno));
				thread_rets->ret = 1;
				break;
			}

			//wait to receive a message from the client
			memset(msg_buf, 0, msg_buf_max);
			status = recv(datacenter_rspd_sock_fd, msg_buf, msg_buf_max, 0);
			if(status != msg_buf_max)
			{
				fprintf(stderr, "%sdatacenter_recv %d encountered an issue while reading the message (status: %d/errno: %d)\n", 
					err_m, id, status, strerror(errno));
				thread_rets->ret = 1;
				break;
			}

			//convert message to an int
			errno = 0;
			clock_inc = strtol(msg_buf, &end, 10);
			if(*end != 0 || errno != 0)
			{
				fprintf(stderr, "%sdatacenter_recv %d encountered an error while converting clock_inc message (errno: %s)", 
					err_m, id, strerror(errno));
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
					err_m, id, status, strerror(errno));
				thread_rets->ret = 1;
				break;
			}
			fprintf(stdout, "%sdatacenter_recv send %s\n", log_m, msg_buf);

			memset(msg_buf, 0, msg_buf_max);
			sprintf(msg_buf, "%d", clock_queue[0]);
			delay(msg_delay);
			status = send(datacenter_rspd_sock_fd, msg_buf, msg_buf_max, 0);
			if(status != msg_buf_max)
			{
				fprintf(stderr, "%sdatacenter_recv %d encoutnered an issue while sending response (status: %d/errno: %d)\n", 
					err_m, id, status, strerror(errno));
				thread_rets->ret = 1;
				break;
			}
			fprintf(stdout, "%sdatacenter_recv send %s\n", log_m, msg_buf);
		}
		else
		{
			fprintf(stdout, "%sdatacenter_recv %d received a connection on comm 1\n", log_m, id);

			//wait to receive a message from the client
			memset(msg_buf, 0, msg_buf_max);
			status = recv(datacenter_rspd_sock_fd, msg_buf, msg_buf_max, 0);
			if(status != msg_buf_max)
			{
				fprintf(stderr, "%sdatacenter_recv %d encountered an issue while reading the message (status: %d/errno: %d)\n", 
					err_m, id, status, strerror(errno));
				thread_rets->ret = 1;
				break;
			}

			//convert message to an int
			errno = 0;
			*requested = strtol(msg_buf, &end, 10);
			if(*end != 0 || errno != 0 || *requested < 0 || *requested > 100)
			{
				fprintf(stderr, "%sdatacenter_recv %d encountered an error while converting request message (errno: %s)", 
					err_m, id, strerror(errno));
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
			fprintf(stdout, "%sthe pool has been updated to contain %d tickets\n", log_m, *pool);
			l_clock += 1;
		}

		//close the datacenter and client socket connections
		status = close(datacenter_rspd_sock_fd);
		if(status < 0)
		{
			fprintf(stderr, "%sdatacenter_recv %d failed to cleanly close the client_rspd socket (status: %d/errno: %d)\n", 
				err_m, id, status, strerror(errno));
			thread_rets->ret = 1;
			break;
		}
		status = close(datacenter_recv_sock_fd);
		if(status < 0)
		{
			fprintf(stderr, "%sdatacenter_recv %d failed to cleanly close the client_recv socket (status: %d/errno: %d)\n", 
				err_m, id, status, strerror(errno));
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
		fprintf(stdout, "%sdatacenter_send %d request_sig has been triggered\n", log_m, id);

		RETRANSMIT:
		for(i = 0; i < count; i++)
		{
			//skip yourself
			if(id == i+1)
			{
				continue;
			}
			fprintf(stdout, "%sdatacenter_send %d sending request message to datacenter #%d\n", log_m, id, i+1);

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
				fprintf(stderr, "%sdatacenter_send failed to open a socket (status: %d/errno: %d)\n", err_m, status, strerror(errno));
				thread_rets->ret = 1;
				break;
			}
			datacenter_send_sock_fd = status;

			//connect to datacenter
			status = connect(datacenter_send_sock_fd, (struct sockaddr*)&datacenter_send_addr, datacenter_send_addr_len);
			if(status < 0)
			{
				fprintf(stdout, "%sdatacenter_send skipping datacenter %d since it's not online\n", log_m, i+1);
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
				fprintf(stdout, "%sdatacenter_send was interrupted while sending comma, retrying...\n", log_m);
				status = close(datacenter_send_sock_fd);
				if(status < 0)
				{
					fprintf(stderr, "%sdatacenter_send failed to cleanly close the socket (status: %d/errno: %d)\n", 
						err_m, status, strerror(errno));
					thread_rets->ret = 1;
					break;
				}
				//open a datacenter_send socket
				status = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
				if(status < 0)
				{
					fprintf(stderr, "%sdatacenter_send failed to open a socket (status: %d/errno: %d)\n", err_m, status, strerror(errno));
					thread_rets->ret = 1;
					break;
				}
				datacenter_send_sock_fd = status;
				//connect to datacenter
				status = connect(datacenter_send_sock_fd, (struct sockaddr*)&datacenter_send_addr, datacenter_send_addr_len);
				if(status < 0)
				{
					fprintf(stdout, "%sdatacenter_send skipping datacenter %d since it's not online\n", log_m, i+1);
					continue;
				}
				goto RETRY_COMMA_SEND;
			}
			else if(status != msg_buf_max)
			{
				fprintf(stderr, "%sdatacenter_send encoutnered an issue while sending request (status: %d/errno: %d)\n", 
					err_m, status, strerror(errno));
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
				fprintf(stdout, "%sdatacenter_send was interrupted while sending id, retrying...\n", log_m);
				status = close(datacenter_send_sock_fd);
				if(status < 0)
				{
					fprintf(stderr, "%sdatacenter_send failed to cleanly close the socket (status: %d/errno: %d)\n", 
						err_m, status, strerror(errno));
					thread_rets->ret = 1;
					break;
				}
				//open a datacenter_send socket
				status = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
				if(status < 0)
				{
					fprintf(stderr, "%sdatacenter_send failed to open a socket (status: %d/errno: %d)\n", err_m, status, strerror(errno));
					thread_rets->ret = 1;
					break;
				}
				datacenter_send_sock_fd = status;
				//connect to datacenter
				status = connect(datacenter_send_sock_fd, (struct sockaddr*)&datacenter_send_addr, datacenter_send_addr_len);
				if(status < 0)
				{
					fprintf(stdout, "%sdatacenter_send skipping datacenter %d since it's not online\n", log_m, i+1);
					continue;
				}
				goto RETRY_ID_SEND;
			}
			else if(status != msg_buf_max)
			{
				fprintf(stderr, "%sdatacenter_send encoutnered an issue while sending request (status: %d/errno: %d)\n", 
					err_m, status, strerror(errno));
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
				fprintf(stdout, "%sdatacenter_send was interrupted while sending clock, retrying...\n", log_m);
				status = close(datacenter_send_sock_fd);
				if(status < 0)
				{
					fprintf(stderr, "%sdatacenter_send failed to cleanly close the socket (status: %d/errno: %d)\n", 
						err_m, status, strerror(errno));
					thread_rets->ret = 1;
					break;
				}
				//open a datacenter_send socket
				status = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
				if(status < 0)
				{
					fprintf(stderr, "%sdatacenter_send failed to open a socket (status: %d/errno: %d)\n", err_m, status, strerror(errno));
					thread_rets->ret = 1;
					break;
				}
				datacenter_send_sock_fd = status;
				//connect to datacenter
				status = connect(datacenter_send_sock_fd, (struct sockaddr*)&datacenter_send_addr, datacenter_send_addr_len);
				if(status < 0)
				{
					fprintf(stdout, "%sdatacenter_send skipping datacenter %d since it's not online\n", log_m, i+1);
					continue;
				}
				goto RETRY_CLOCK_SEND;
			}
			else if(status != msg_buf_max)
			{
				fprintf(stderr, "%sdatacenter_send encoutnered an issue while sending request (status: %d/errno: %d)\n", 
					err_m, status, strerror(errno));
				thread_rets->ret = 1;
				break;
			}
	
			//wait to receive a response from the datacenter
			RETRY_ID_RECV:
			status = recv(datacenter_send_sock_fd, msg_buf, msg_buf_max, 0);
			if(status == -1 && errno == 104)
			{
				//close the socket connection to the datacenter and try again
				fprintf(stdout, "%sdatacenter_send was interrupted while receving id, retrying...\n", log_m);
				status = close(datacenter_send_sock_fd);
				if(status < 0)
				{
					fprintf(stderr, "%sdatacenter_send failed to cleanly close the socket (status: %d/errno: %d)\n", 
						err_m, status, strerror(errno));
					thread_rets->ret = 1;
					break;
				}
				//open a datacenter_send socket
				status = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
				if(status < 0)
				{
					fprintf(stderr, "%sdatacenter_send failed to open a socket (status: %d/errno: %d)\n", err_m, status, strerror(errno));
					thread_rets->ret = 1;
					break;
				}
				datacenter_send_sock_fd = status;
				//connect to datacenter
				status = connect(datacenter_send_sock_fd, (struct sockaddr*)&datacenter_send_addr, datacenter_send_addr_len);
				if(status < 0)
				{
					fprintf(stdout, "%sdatacenter_send skipping datacenter %d since it's not online\n", log_m, i+1);
					continue;
				}
				goto RETRY_ID_RECV;
			}
			else if(status != msg_buf_max)
			{
				fprintf(stderr, "%sdatacenter_send encountered an issue while reading the response (status: %d/errno: %d)\n", 
					err_m, status, strerror(errno));
				thread_rets->ret = 1;
				break;
			}

			//convert response to an int
			errno = 0;
			id_queue_res[i] = strtol(msg_buf, &end, 10);
			if(*end != 0 || errno != 0)
			{
				fprintf(stderr, "%sdatacenter_send encountered an error while converting response id (errno: %s)", 
					err_m, strerror(errno));
				thread_rets->ret = 1;
				break;
			}

			//wait to receive a response from the datacenter
			RETRY_CLOCK_RECV:
			status = recv(datacenter_send_sock_fd, msg_buf, msg_buf_max, 0);
			if(status == -1 && errno == 104)
			{
				//close the socket connection to the datacenter and try again
				fprintf(stdout, "%sdatacenter_send was interrupted while receiving clock, retrying...\n", log_m);
				status = close(datacenter_send_sock_fd);
				if(status < 0)
				{
					fprintf(stderr, "%sdatacenter_send failed to cleanly close the socket (status: %d/errno: %d)\n", 
						err_m, status, strerror(errno));
					thread_rets->ret = 1;
					break;
				}
				//open a datacenter_send socket
				status = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
				if(status < 0)
				{
					fprintf(stderr, "%sdatacenter_send failed to open a socket (status: %d/errno: %d)\n", err_m, status, strerror(errno));
					thread_rets->ret = 1;
					break;
				}
				datacenter_send_sock_fd = status;
				//connect to datacenter
				status = connect(datacenter_send_sock_fd, (struct sockaddr*)&datacenter_send_addr, datacenter_send_addr_len);
				if(status < 0)
				{
					fprintf(stdout, "%sdatacenter_send skipping datacenter %d since it's not online\n", log_m, i+1);
					continue;
				}
				goto RETRY_CLOCK_RECV;
			}
			else if(status != msg_buf_max)
			{
				fprintf(stderr, "%sdatacenter_send encountered an issue while reading the response (status: %d/errno: %d)\n", 
					err_m, status, strerror(errno));
				thread_rets->ret = 1;
				break;
			}

			//convert response to an int
			errno = 0;
			clock_queue_res[i] = strtol(msg_buf, &end, 10);
			if(*end != 0 || errno != 0)
			{
				fprintf(stderr, "%sdatacenter_send encountered an error while converting response clock (errno: %s)", 
					err_m, strerror(errno));
				thread_rets->ret = 1;
				break;
			}

			//close the socket connection to the datacenter
			status = close(datacenter_send_sock_fd);
			if(status < 0)
			{
				fprintf(stderr, "%sdatacenter_send failed to cleanly close the socket (status: %d/errno: %d)\n", 
					err_m, status, strerror(errno));
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
			log_m, l_clock, id, clock_queue_min, id_queue_min);
		if(id_queue_min == id)
		{
			claimed_sig = 1;
		}
		else
		{
			//retransmit the request
			fprintf(stdout, "%sdatacenter_send waiting for hold claim on queue, retransmitting...\n", log_m);
			retransmit = 1;
			memset(clock_queue_res, 0, sizeof(clock_queue_res));
			memset(id_queue_res, 0, sizeof(id_queue_res));
			goto RETRANSMIT;
		}

		thread_rets->ret = 0;
		while(claimed_sig == 0);
		retransmit = 0;
		fprintf(stdout, "%sdatacenter_send claimed_sig has been triggered\n", log_m);

		for(i = 0; i < count; i++)
		{
			//skip yourself
			if(id == i+1)
			{
				continue;
			}
			fprintf(stdout, "%sdatacenter_send sending release message to datacenter #%d\n", log_m, i+1);

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
				fprintf(stderr, "%sdatacenter_send failed to open a socket (status: %d/errno: %d)\n", err_m, status, strerror(errno));
				thread_rets->ret = 1;
				break;
			}
			datacenter_send_sock_fd = status;

			//connect to datacenter
			status = connect(datacenter_send_sock_fd, (struct sockaddr*)&datacenter_send_addr, datacenter_send_addr_len);
			if(status < 0)
			{
				fprintf(stdout, "%sdatacenter_send skipping datacenter %d since it's not online\n", log_m, i+1);
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
				fprintf(stdout, "%sdatacenter_send was interrupted while sending commb, retrying...\n", log_m);
				status = close(datacenter_send_sock_fd);
				if(status < 0)
				{
					fprintf(stderr, "%sdatacenter_send failed to cleanly close the socket (status: %d/errno: %d)\n", 
						err_m, status, strerror(errno));
					thread_rets->ret = 1;
					break;
				}
				//open a datacenter_send socket
				status = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
				if(status < 0)
				{
					fprintf(stderr, "%sdatacenter_send failed to open a socket (status: %d/errno: %d)\n", err_m, status, strerror(errno));
					thread_rets->ret = 1;
					break;
				}
				datacenter_send_sock_fd = status;
				//connect to datacenter
				status = connect(datacenter_send_sock_fd, (struct sockaddr*)&datacenter_send_addr, datacenter_send_addr_len);
				if(status < 0)
				{
					fprintf(stdout, "%sdatacenter_send skipping datacenter %d since it's not online\n", log_m, i+1);
					continue;
				}
				goto RETRY_COMMB_SEND;
			}
			else if(status != msg_buf_max)
			{
				fprintf(stderr, "%sdatacenter_send encoutnered an issue while sending request (status: %d/errno: %d)\n", 
					err_m, status, strerror(errno));
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
					log_m);
				status = close(datacenter_send_sock_fd);
				if(status < 0)
				{
					fprintf(stderr, "%sdatacenter_send failed to cleanly close the socket (status: %d/errno: %d)\n", 
						err_m, status, strerror(errno));
					thread_rets->ret = 1;
					break;
				}
				//open a datacenter_send socket
				status = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
				if(status < 0)
				{
					fprintf(stderr, "%sdatacenter_send failed to open a socket (status: %d/errno: %d)\n", err_m, status, strerror(errno));
					thread_rets->ret = 1;
					break;
				}
				datacenter_send_sock_fd = status;
				//connect to datacenter
				status = connect(datacenter_send_sock_fd, (struct sockaddr*)&datacenter_send_addr, datacenter_send_addr_len);
				if(status < 0)
				{
					fprintf(stdout, "%sdatacenter_send skipping datacenter %d since it's not online\n", log_m, i+1);
					continue;
				}
				goto RETRY_TICKETR_SEND;
			}
			else if(status != msg_buf_max)
			{
				fprintf(stderr, "%sdatacenter_send encoutnered an issue while sending request (status: %d/errno: %d)\n", 
					err_m, status, strerror(errno));
				thread_rets->ret = 1;
				break;
			}

			//close the socket connection to the datacenter
			status = close(datacenter_send_sock_fd);
			if(status < 0)
			{
				fprintf(stderr, "%sdatacenter_send failed to cleanly close the socket (status: %d/errno: %d)\n", 
					err_m, status, strerror(errno));
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
}*/