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
	int32_t init_this_dc = 0;
	int32_t i;
	int32_t status;
	int32_t dc_addr_count;
	int32_t dc_addr_id;
	int32_t ticket_pool_init;
	int32_t msg_delay;
	int32_t cl_lstn_port;
	int32_t dc_lstn_port_base;
	config_t cf_local;
	config_t *cf;
	struct flock *fl;
	int32_t fd;
	time_t boot_time;
	char stdin_buf[256];
	config_setting_t *dc_addr_settings;
	config_setting_t *dc_addr_elem_setting;
	pthread_t cl_lstn_thread_id;
	pthread_t *dc_bcst_thread_id;
	// pthread_t dc_lstn_thread_id;


	cl_lstn_arg_t *cl_lstn_args;
	dc_bcst_arg_t *dc_bcst_args;
	// arg_t *dc_lstn_args;

	ret_t *cl_lstn_rets;
	ret_t *dc_bcst_rets;


	/*
		REVISED CODE
	*/
	dc_init();

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
		pthread_mutex_lock(&this_clk.lock);
		fprintf(stderr, "%s%d%s (%s) encountered an issue while reading from config file: %s:%d - %s\n", 
			err_m, this_clk.clk, cls_m, fnc_m, config_error_file(cf), config_error_line(cf), config_error_text(cf));
		pthread_mutex_unlock(&this_clk.lock);
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
	dc_sys = (dc_sys_t *)malloc(dc_addr_count * sizeof(dc_sys_t));
	for(i = 0; i < dc_addr_count; i++)
	{
		dc_addr_elem_setting = config_setting_get_elem(dc_addr_settings, i);
		config_setting_lookup_int(dc_addr_elem_setting, "id", &(dc_sys[i].id));
		config_setting_lookup_int(dc_addr_elem_setting, "online", &(dc_sys[i].online));
		config_setting_lookup_string(dc_addr_elem_setting, "hostname", &hn);
		dc_sys[i].hostname = (char *)malloc(strlen(hn) + 1);
		memcpy(dc_sys[i].hostname, hn, strlen(hn) + 1);
		pthread_mutex_init(&(dc_sys[i].lock), NULL);

		//set the index for the first availble datacenter not currently online
		if(!dc_sys[i].online && !init_this_dc)
		{
			//set online status in .cfg to 1, initialize this_dc
			config_setting_set_int(config_setting_get_member(dc_addr_elem_setting, "online"), 1);
			config_write_file(cf, "global.cfg");
			dc_sys[i].online = 1;
			this_dc.id = dc_sys[i].id;
			this_dc.online = dc_sys[i].online;
			this_dc.hostname = dc_sys[i].hostname;
			init_this_dc = 1;
		}
	}

	//all datacenters are already online
	if(!init_this_dc)
	{
		config_destroy(cf);
		unlock_cfg(fd, fl);
		close(fd);
		return dc_log(stdout, "%s%d%s (%s) can't start another datacenter, all datacenters specified by global.cfg are currently online\n", log_m, fnc_m, 0);
	}

	//Close and unlock the config file
	config_destroy(cf);
	unlock_cfg(fd, fl);
	close(fd);

	//log datacenter metadata
	time(&boot_time);
	fprintf(stdout, "========================\n|   Datacenter #%d Log   |\n========================\n\n", this_dc.id);
	fprintf(stdout, "- Boot Time: %s\n", asctime(localtime(&boot_time)));
	fprintf(stdout, "- ID: %d\n", this_dc.id);
	fprintf(stdout, "- Initial Clock: %d\n", this_dc.clk);
	fprintf(stdout, "- Initial Ticket Pool: %d\n", ticket_pool_init);
	fprintf(stdout, "- Current Ticket Pool: %d\n", ticket_pool);
	fprintf(stdout, "- Client Listen Port: %d\n", cl_lstn_port);
	fprintf(stdout, "- Datacenter Broadcast Base Port: %d\n",dc_lstn_port_base);
	fprintf(stdout, "- Datacenter Hostname: %s\n\n\n", this_dc.hostname);

	//SPAWN DC SERVER

	//wait for user input to connect to other datacenters
	fprintf(stdout, "Please press the Enter key to connect to all other online datacenters.\n>>>", );
	fgets(stdin_buf, sizeof(stdin_buf), stdin);
	fprintf(stdout, "\nNow connecting...\n", );

	//create threads for broadcasting online/request/release packets
	dc_bcst_thread_ids = (pthread_t *)malloc((dc_addr_count)*sizeof(pthread_t));
	fflush_out_err();
	for(i = 0; i < dc_addr_count; i++)
	{
		//don't broadcast message to self
		if(dc_sys[i].id == this_dc.id)
		{
			continue;
		}
		dc_bcst_args = (dc_bcst_arg_t *)malloc(sizeof(dc_bcst_arg_t));
		dc_bcst_args->dest_id = dc_sys[i].id;
		dc_bcst_args->port_base = dc_lstn_port_base;
		fflush_out_err();
		status = pthread_create(&(dc_bcst_thread_ids[i]), NULL, dc_bcst_thread, (void *)dc_bcst_args);
		if(status != 0)
		{
			dc_log(stderr, "%s%d%s (%s) failed to spawn dc_bcst_thread (errno: %s)\n", err_m, fnc_m, 1);
			return 1;
		}
		pthread_mutex_lock(&pool_lock);
	}

	//update the number of other datacenters online
	for(i = 0; i < dc_addr_count; i++)
	{
		pthread_mutex_lock(&(dc_sys[i].lock));
		//don't count self
		if(dc_sys[i].id == this_dc.id)
		{
			pthread_mutex_unlock(&(dc_sys[i].lock));
			continue;
		}
		if(dc_sys[i].online == 1)
		{
			dc_sys_online++;
		}
		pthread_mutex_unlock(&(dc_sys[i].lock));
	}

	//spawn cl_lstn_thread
	cl_lstn_args = (arg_t *)malloc(sizeof(arg_t));
	cl_lstn_args->count = dc_addr_count;
	cl_lstn_args->port = cl_lstn_port;
	fflush_out_err();
	status = pthread_create(&cl_lstn_thread_id, NULL, cl_lstn_thread, (void *)cl_lstn_args);
	if(status != 0)
	{
		return dc_log(stderr, "%s%d%s (%s) failed to spawn cl_lstn_thread (errno: %s)\n", err_m, fnc_m, 1);
	}

/*	//spawn datacenter_recv thread
	datacenter_recv_args = (arg_t *)malloc(sizeof(struct arg_struct));
	datacenter_recv_args->id = datacenter.id;
	datacenter_recv_args->pool = &ticket_pool;
	datacenter_recv_args->requested = &tickets_requested;
	datacenter_recv_args->delay = msg_delay;
	datacenter_recv_args->count = datacenter_count;
	datacenter_recv_args->port = datacenter_recv_port;
	datacenter_recv_args->hostname = datacenter.hostname;
	fflush_out_err();
	status = pthread_create(&datacenter_recv_thread_id, NULL, datacenter_recv_thread, (void *)datacenter_recv_args);
	if(status != 0)
	{
		fprintf(stderr, "%sdatacenter %d failed to spawn datacenter_recv thread (status: %d/errno: %d)\n", 
			err_m, datacenter.id, status, strerror(errno));
		return 1;
	}
*/
	//spawn datacenter_send thread
	dc_bcst_args = (arg_t *)malloc(sizeof(struct arg_struct));
	dc_bcst_args->count = dc_addr_count;
	dc_bcst_args->port = dc_lstn_port_base;
	fflush_out_err();
	status = pthread_create(&dc_bcst_thread_id, NULL, dc_bcst_thread, (void *)dc_bcst_args);
	if(status != 0)
	{
		return dc_log(stderr, "%sdatacenter %d failed to spawn datacenter_send thread (status: %d/errno: %d)\n", err_m, fnc_m, 1);
	}

	dc_log(stdout, "%s%d%s (%s) finished spawning primary child threads\n", log_m, fnc_m, 0);


	//JOIN THREADS

	//join threads once all broadcast ipc has completed
	for(i = 0; i < dc_addr_count; i++)
	{
		fflush_out_err();
		status = pthread_join(dc_bcst_thread_ids[i], (void **)&dc_bcst_rets);
		fflush_out_err();
		ret += dc_bcst_rets->ret;
		free(dc_bcst_rets);
	}
	free(dc_bcst_thread_ids);


	dc_log(stdout, "%s%d%s (%s) finished joining primary child threads\n", log_m, fnc_m, 0);

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
		pthread_mutex_lock(&this_clk.lock);
		fprintf(stderr, "%s%d%s (%s) encountered an issue while reading from config file: %s:%d - %s\n", 
			err_m, this_clk.clk, cls_m, fnc_m, config_error_file(cf), config_error_line(cf), config_error_text(cf));
		pthread_mutex_unlock(&this_clk.lock);
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
		if(dc_addr_id == this_dc.id)
		{
			config_setting_set_int(config_setting_get_member(dc_addr_elem_setting, "online"), 0);
			config_write_file(cf, "global.cfg");
			break;
		}
	}

	//Close and unlock the config file
	config_destroy(cf);
	unlock_cfg(fd, fl);
	close(fd);

	return 0;
}

void dc_init()
{
	pthread_mutex_init(&pool_lock, NULL);
	pthread_mutex_init(&bcst_lock, NULL);
	pthread_mutex_init(&(this_clk.lock), NULL);
	pthread_mutex_lock(&pool_lock);
	pthread_mutex_lock(&bcst_lock);
	this_clk.clk = 0;
	dc_sys_online = 0;
}

int32_t dc_log(FILE *std_strm, char *msg, char *opn_m char *fnc_m, int32_t errrno_f)
{
	pthread_mutex_lock(this_clk.clk);
	if(errno_f)
	{
		fprintf(std_strm, msg, opn_m, this_clk.clk, cls_m, fnc_m, strerror(errno));
	}
	else
	{
		fprintf(std_strm, msg, opn_m, this_clk.clk, cls_m, fnc_m);
	}
	pthread_mutex_unlock(this_clk.clk);
	fflush_out_err();
	return 1;
}

uint8_t *packet_stream encode_packet(int32_t p_type, int32_t p_id, int32_t p_clk, int32_t p_pool)
{
	packet_t packet;
	uint8_t *packet_stream = (uint8_t *)malloc(sizeof(packet));

	packet.type = hton32((uint32_t)p_type);
	packet.id = hton32((uint32_t)p_id);
	packet.clk = hton32((uint32_t)p_clk);
	packet.pool = hton32((uint32_t)p_pool);

	packet_stream = (uint8_t *)packet;
	return packet_stream;
}

packet_t *packet decode_packet(uint8_t *packet_stream)
{
	packet_t *packet = (packet_t *)malloc(sizeof(packet));

	packet = (packet_t *)packet_stream;
	packet->type = ntoh32(packet->type);
	packet->id = ntoh32(packet->id);
	packet->clk = ntoh32(packet->clk);
	packet->pool = ntoh32(packet->pool);

	return packet;
}

//acts as a frontend for communicating with the client
void *cl_lstn_thread(void *args)
{
	int32_t msg_buf_max = 4096;
	int32_t sockopt = 1;
	int32_t status;
	int32_t cl_lstn_sock_fd;
	int32_t cl_rspd_sock_fd;
	int32_t count;
	int32_t port;
	int32_t ticket_amount;
	char msg_buf[msg_buf_max];
	char *fnc_m = "cl_lstn_thread";
	char *end;
	struct sockaddr_in cl_lstn_addr;
	struct sockaddr_in cl_rspd_addr;
	socklen_t cl_lstn_addr_len;
	socklen_t cl_rspd_addr_len;
	cl_lstn_arg_t *thread_args = (cl_lstn_arg_t *)args;
	ret_t *thread_rets = (ret_t *)malloc(sizeof(ret_obj));

	//read out data packed in the args parameter
	fflush_out_err();
	count = thread_args->count;
	port = thread_args->port;
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
		thread_rets->ret = dc_log(stderr, "%s%d%s (%s) failed to open a socket (errno: %s)\n", err_m, fnc_m, 1);
		pthread_exit((void *)thread_rets);
	}
	cl_lstn_sock_fd = status;
	dc_log(stdout, "%s%d%s (%s) finished initializing a socket for incoming client connections\n", log_m, fnc_m, 0);

	//check if the system is still holding onto the port after a recent restart
	status = setsockopt(cl_lstn_sock_fd, SOL_SOCKET, SO_REUSEADDR, (const void *)&sockopt, sizeof(int));
	if(status < 0)
	{
		thread_rets->ret = dc_log(stderr, "%s%d%s (%s) can't bind to port due to a recent program restart (errno: %s)\n", err_m, fnc_m, 1);
		pthread_exit((void *)thread_rets);
	}
	dc_log(stdout, "%s%d%s (%s) finished setting the socket to be reusable\n", log_m, fnc_m, 0);

	//bind to a port on the datacenter
	status = bind(cl_lstn_sock_fd, (struct sockaddr *)&cl_lstn_addr, cl_lstn_addr_len);
	if(status < 0)
	{
		thread_rets->ret = dc_log(stderr, "%s%d%s (%s) failed to bind to the specified port (errno: %s)\n", err_m, fnc_m, 1);
		pthread_exit((void *)thread_rets);
	}
	dc_log(stdout, "%s%d%s (%s) finished binding to the socket\n", log_m, fnc_m, 0);

	//listen to the port for an incoming client connection
	status = listen(cl_lstn_sock_fd, 1);
	if(status < 0)
	{
		thread_rets->ret = dc_log(stderr, "%s%d%s (%s) failed to listen to the port for clients (errno: %s)\n", err_m, fnc_m, 1);
		pthread_exit((void *)thread_rets);
	}
	cl_rspd_addr_len = sizeof(cl_rspd_addr);
	dc_log(stdout, "%s%d%s (%s) now listening for client connections\n", log_m, fnc_m, 0);

	//continuosly accept new client connections until an error occurs or the thread is ended
	while(1)
	{
		pthread_mutex_lock(&(ticket_pool.lock));
		pthread_mutex_lock(&(this_clk.lock));
		fprintf(stdout, "%s%d%s (%s) ticket pool amount before accepting a new client: %d\n", 
			log_m, this_clk.clk, cls_m, fnc_m, ticket_pool.pool);
		pthread_mutex_unlock(&(this_clk.lock));
		pthread_mutex_unlock(&(ticket_pool.lock));

		//accept connection from client
		status = accept(cl_lstn_sock_fd, (struct sockaddr *)&cl_rspd_addr, &ccl_rspd_addr_len);
		if(status < 0)
		{
			thread_rets->ret = dc_log(stderr, "%s%d%s (%s) failed to accept a connection from a client (errno: %s)\n", err_m, fnc_m, 1);
			pthread_exit((void *)thread_rets);
		}
		cl_rspd_sock_fd = status;
		dc_log(stdout, "%s%d%s (%s) accepted a new client connection\n", log_m, fnc_m, 0);

		//wait to receive a message from the client
		memset(msg_buf, 0, msg_buf_max);
		status = recv(cl_rspd_sock_fd, msg_buf, msg_buf_max, 0);
		if(status != msg_buf_max)
		{
			thread_rets->ret = dc_log(stderr, "%s%d%s (%s) encountered an issue while reading the message (errno: %d)\n", err_m, fnc_m, 1);
			pthread_exit((void *)thread_rets);
		}
		dc_log(stdout, "%s%d%s (%s) received a request from a client\n", log_m, fnc_m, 0);

		//convert message to an int
		errno = 0;
		ticket_amount = strtol(msg_buf, &end, 10);
		if(*end != 0 || errno != 0)
		{
			thread_rets->ret = dc_log(stderr, "%s%d%s (%s) encountered an error while converting client ticket request message (errno: %s)\n", err_m, fnc_m, 1);
			pthread_exit((void *)thread_rets);
		}

		//WAIT FOR THREADS TO NEGOTIATE ACCESS TO TICKET POOL
		for(i = 0; i < dc_sys_online; i++)
		{
			pthread_mutex_unlock(&bcst_lock);
			pthread_mutex_lock(&pool_lock);
		}

		dc_log(stdout, "%s%d%s (%s) ticket pool control has been obtained, processing request\n", log_m, fnc_m, 0);

		//set a flag indicating whether the request message was accepted
		pthread_mutex_lock(&(ticket_pool.lock));
		pthread_mutex_lock(&this_clk.lock);
		memset(msg_buf, 0, msg_buf_max);
		if(ticket_amount > ticket_pool.pool)
		{
			strcpy(msg_buf, "0");
			fprintf(stdout, "%s%d%s (%s) rejected a request from a client for %d ", ticket_amount);
			print_tickets(ticket_amount);
			fprintf(stdout, " (%d ", ticket_pool.pool);
			print_tickets(ticket_pool);
			fprintf(stdout, " remaining in the pool)\n");
		}
		else
		{
			ticket_pool.pool -= ticket_amount;
			strcpy(msg_buf, "1");
			fprintf(stdout, "%s%d%s (%s) accepted a request from a client for %d ", ticket_amount);
			print_tickets(ticket_amount);
			fprintf(stdout, " (%d ", ticket_pool.pool);
			print_tickets(ticket_pool);
			fprintf(stdout, " remaining in the pool)\n");
		}
		thread_mutex_unlock(&this_clk.lock);
		pthread_mutex_unlock(&(ticket_pool.lock));
		dc_log(stdout, "%s%d%s (%s) request completed, allowing release of ticket pool control\n", log_m, fnc_m, 0);

		//ALLOW THREADS TO RELEASE ACCESS OF TICKET POOL
		for(i = 0; i < dc_sys_online; i++)
		{
			pthread_mutex_unlock(&bcst_lock);
			pthread_mutex_lock(&pool_lock);
		}

		//send the client the request results
		status = send(cl_rspd_sock_fd, msg_buf, msg_buf_max, 0);
		if(status != msg_buf_max)
		{
			thread_rets->ret = dc_log(stderr, "%s%d%s (%s) encountered an issue while sending the message (errno: %s)\n", err_m, fnc_m, 1);
			pthread_exit((void *)thread_rets);;
		}
		dc_log(stdout, "%s%d%s (%s) sent the request results back to the client\n", log_m, fnc_m, 0);

		//close the response socket for the client
		status = close(cl_rspd_sock_fd);
		if(status < 0)
		{
			thread_rets->ret = dc_log(stderr, "%s%d%s (%s) failed to cleanly close the client response socket (errno: %s)\n", err_m, fnc_m, 1);
			pthread_exit((void *)thread_rets);
		}
		dc_log(stdout, "%s%d%s (%s) closed the response point of the client connection\n", log_m, fnc_m, 0);
	}

	//close the datacenter's socket for incoming client requests
	status = close(cl_lstn_sock_fd);
	if(status < 0)
	{
		thread_rets->ret = dc_log(stderr, "%s%d%s (%s) failed to cleanly close the datacenter's socket for client requests (errno: %s)\n", err_m, fnc_m, 1);
		pthread_exit((void *)thread_rets);
	}
	dc_log(stdout, "%s%d%s (%s) closed the datacenter's socket for client requests\n", log_m, fnc_m, 0);

	fflush_out_err();
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
}*/

//send messages to all other datacenters for synchronization
void *dc_bcst_thread(void *args)
{
	int32_t i;
	int32_t status;
	int32_t ret;
	int32_t dest_id;
	int32_t port_base;
	int32_t dc_bcst_sock_fd;
	char *fnc_m = "dc_bcst_thread";
	uint8_t *packet_stream;
	packet_t *packet;
	struct sockaddr_in dc_bcst_addr;
	socklen_t dc_bcst_addr_len;
	dc_send_arg_t *dc_send_args;
	ret_t *dc_send_rets;
	
	//unpack args
	fflush_out_err();
	dest_id = thread_args->dest_id;
	port_base = thread_args->port_base;
	free(thread_args);
	thread_rets->ret = 0;

	//setup address to connect to the datacenter of dest_id on the port associated with this_dc's id
	dc_bcst_addr_len = sizeof(dc_bcst_addr);
	memset((char *)&dc_bcst_addr, 0, dc_bcst_addr_len);
	dc_bcst_addr.sin_family = AF_INET;
	dc_bcst_addr.sin_addr.s_addr = inet_addr(dc_sys[dest_id-1].hostname);
	dc_bcst_addr.sin_port = htons(port_base + this_dc.id);

	//open a server socket in the datacenter to connect to other datacenter servers
	status = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
	if(status < 0)
	{
		thread_rets->ret = dc_log(stderr, "%s%d%s (%s) failed to open a socket (errno: %s)\n", err_m, fnc_m, 1);
		pthread_exit((void *)thread_rets);
	}
	dc_bcst_sock_fd = status;
	dc_log(stdout, "%s%d%s (%s) finished initializing a socket for an outgoing server connection\n", log_m, fnc_m, 0);

	//connect to the datacenter
	status = connect(dc_bcst_sock_fd, (struct sockaddr*)&dc_bcst_addr, dc_bcst_addr_len);
	if(status < 0)
	{
		//datacenter was offline
		thread_rets->ret = dc_log(stderr, "%s%d%s (%s) failed to connect to the datacenter server (errno: %s)\n", err_m, fnc_m, 1);
		pthread_mutex_lock(&(dc_sys[dest_id-1].lock));
		dc_sys[dest_id-1].online = 0;
		pthread_mutex_unlock(&(dc_sys[dest_id-1].lock));
		pthread_mutex_unlock(&pool_lock);
		pthread_exit((void *)thread_rets);
	}

	//build a packet signaling this datacenter is now online
	pthread_mutex_lock(&(ticket_pool.lock));
	pthread_mutex_lock(&(this_clk.lock));
	packet_stream = encode_packet(ONLINE, this_dc.id, this_clk.clk, ticket_pool.pool);

	//send the datacenter the online packet
	status = send(dc_bcst_sock_fd, packet_stream, sizeof(packet_stream), 0);
	if(status != sizeof(packet_stream))
	{
		pthread_mutex_unlock(&(this_clk.lock));
		thread_rets->ret = dc_log(stderr, "%s%d%s (%s) encountered an issue while sending the online packet (errno: %s)\n", err_m, fnc_m, 1);
		pthread_exit((void *)thread_rets);
	}
	free(packet_stream);

	//receive the ack packet back from the server
	status = recv(dc_bcst_sock_fd, packet_stream, sizeof(packet_stream), 0);
	if(status != sizeof(packet_stream))
	{
		pthread_mutex_unlock(&(this_clk.lock));
		thread_rets->ret = dc_log(stderr, "%s%d%s (%s) encountered an issue while receving the ack packet for the online packet (errno: %d)\n", err_m, fnc_m, 1);
		pthread_exit((void *)thread_rets);
	}

	//decode packet, make sure online signal was ack'd
	packet = decode_packet(packet_stream);
	if(packet->id != this_dc.id || packet->type != ACK)
	{
		pthread_mutex_unlock(&(this_clk.lock));
		thread_rets->ret = dc_log(stderr, "%s%d%s (%s) received an invalid ack packet for the online packet\n", err_m, fnc_m, 0);
		pthread_exit((void *)thread_rets);
	}
	free(packet);
	pthread_mutex_lock(&(dc_sys[dest_id-1].lock));
	dc_sys[dest_id-1].online = 1;
	pthread_mutex_unlock(&(dc_sys[dest_id-1].lock));
	pthread_mutex_unlock(&(this_clk.lock));
	pthread_mutex_unlock(&(ticket_pool.lock));
	pthread_mutex_unlock(&pool_lock);

	//handle request/release communication per client transaction
	while(1)
	{
		pthread_mutex_lock(&bcst_lock);

		//build a packet signaling this datacenter is requesting control of the ticket pool
		pthread_mutex_lock(&(ticket_pool.lock));
		pthread_mutex_lock(&(this_clk.lock));
		packet_stream = encode_packet(REQUEST, this_dc.id, this_clk.clk, ticket_pool.pool);

		//send the datacenter the request packet
		status = send(dc_bcst_sock_fd, packet_stream, sizeof(packet_stream), 0);
		if(status != sizeof(packet_stream))
		{
			pthread_mutex_unlock(&(this_clk.lock));
			thread_rets->ret = dc_log(stderr, "%s%d%s (%s) encountered an issue while sending the request packet (errno: %s)\n", err_m, fnc_m, 1);
			pthread_exit((void *)thread_rets);
		}
		free(packet_stream);

		//receive the ack packet back from the server
		status = recv(dc_bcst_sock_fd, packet_stream, sizeof(packet_stream), 0);
		if(status != sizeof(packet_stream))
		{
			pthread_mutex_unlock(&(this_clk.lock));
			thread_rets->ret = dc_log(stderr, "%s%d%s (%s) encountered an issue while receving the ack packet for the request packet (errno: %d)\n", err_m, fnc_m, 1);
			pthread_exit((void *)thread_rets);
		}

		//decode packet, make sure request signal was ack'd
		packet = decode_packet(packet_stream);
		if(packet->id != this_dc.id || packet->type != ACK)
		{
			pthread_mutex_unlock(&(this_clk.lock));
			thread_rets->ret = dc_log(stderr, "%s%d%s (%s) received an invalid ack packet for the request packet\n", err_m, fnc_m, 0);
			pthread_exit((void *)thread_rets);
		}
		free(packet);
		pthread_mutex_unlock(&(this_clk.lock));
		pthread_mutex_unlock(&(ticket_pool.lock));

		//HAVE CONTROL OF POOL
		pthread_mutex_unlock(&pool_lock);
		pthread_mutex_lock(&bcst_lock);

		//build a packet signaling this datacenter is releasing control of the ticket pool
		pthread_mutex_lock(&(ticket_pool.lock));
		pthread_mutex_lock(&(this_clk.lock));
		packet_stream = encode_packet(RELEASE, this_dc.id, this_clk.clk, ticket_pool.pool);

		//send the datacenter the release packet
		status = send(dc_bcst_sock_fd, packet_stream, sizeof(packet_stream), 0);
		if(status != sizeof(packet_stream))
		{
			pthread_mutex_unlock(&(this_clk.lock));
			thread_rets->ret = dc_log(stderr, "%s%d%s (%s) encountered an issue while sending the release packet (errno: %s)\n", err_m, fnc_m, 1);
			pthread_exit((void *)thread_rets);
		}
		free(packet_stream);

		//receive the ack packet back from the server
		status = recv(dc_bcst_sock_fd, packet_stream, sizeof(packet_stream), 0);
		if(status != sizeof(packet_stream))
		{
			pthread_mutex_unlock(&(this_clk.lock));
			thread_rets->ret = dc_log(stderr, "%s%d%s (%s) encountered an issue while receving the ack packet for the release packet (errno: %d)\n", err_m, fnc_m, 1);
			pthread_exit((void *)thread_rets);
		}

		//decode packet, make sure release signal was ack'd
		packet = decode_packet(packet_stream);
		if(packet->id != this_dc.id || packet->type != ACK)
		{
			pthread_mutex_unlock(&(this_clk.lock));
			thread_rets->ret = dc_log(stderr, "%s%d%s (%s) received an invalid ack packet for the release packet\n", err_m, fnc_m, 0);
			pthread_exit((void *)thread_rets);
		}
		free(packet);
		pthread_mutex_unlock(&(this_clk.lock));
		pthread_mutex_unlock(&(ticket_pool.lock));
		pthread_mutex_unlock(&pool_lock);
	}

	fflush_out_err();
	pthread_exit((void *)thread_rets);
}