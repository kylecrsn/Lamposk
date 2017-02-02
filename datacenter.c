#include "datacenter.h"

int dc_handler()
{
	int32_t init_this_dc = 0;
	int32_t i;
	int32_t status;
	int32_t fd;
	int32_t dc_addr_count;
	int32_t dc_addr_id;
	int32_t ticket_pool_init;
	int32_t msg_delay;
	int32_t cl_lstn_port;
	int32_t dc_lstn_port_base;
	char *fnc_m = "dc_handler";
	const char *hostname;
	//char stdin_buf[256];
	time_t boot_time;
	struct flock *fl;
	config_t cf_local;
	config_t *cf;
	config_setting_t *dc_addr_settings;
	config_setting_t *dc_addr_elem_setting;
	pthread_t cl_lstn_thread_id;
	pthread_t *dc_bcst_thread_ids;
	pthread_t *dc_lstn_thread_ids;
	cl_lstn_arg_t *cl_lstn_args;
	dc_bcst_arg_t *dc_bcst_args;
	dc_lstn_arg_t *dc_lstn_args;
	ret_t *cl_lstn_rets;
	ret_t *dc_bcst_rets;
	ret_t *dc_lstn_rets;

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
	config_lookup_int(cf, "dc.ticket_pool", &(ticket_pool.pool));
	config_lookup_int(cf, "dc.cl_lstn_port", &cl_lstn_port);
	config_lookup_int(cf, "dc.dc_lstn_port_base", &dc_lstn_port_base);
	dc_addr_settings = config_lookup(cf, "dc.addrs");
	dc_addr_count = config_setting_length(dc_addr_settings);

	//populate an array with all of the datacenter configs
	dc_sys = (dc_t *)malloc(dc_addr_count*sizeof(dc_t));
	for(i = 0; i < dc_addr_count; i++)
	{
		dc_addr_elem_setting = config_setting_get_elem(dc_addr_settings, i);
		config_setting_lookup_int(dc_addr_elem_setting, "id", &(dc_sys[i].id));
		config_setting_lookup_int(dc_addr_elem_setting, "online", &(dc_sys[i].online));
		config_setting_lookup_string(dc_addr_elem_setting, "hostname", &hostname);
		dc_sys[i].hostname = (char *)malloc(strlen(hostname) + 1);
		memcpy(dc_sys[i].hostname, hostname, strlen(hostname) + 1);
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
	fprintf(stdout, "=========================\n|   Datacenter #%d Log   |\n=========================\n", this_dc.id);
	fprintf(stdout, "- Boot Time: %s", asctime(localtime(&boot_time)));
	fprintf(stdout, "- ID: %d\n", this_dc.id);
	fprintf(stdout, "- Initial Clock: %d\n", this_clk.clk);
	fprintf(stdout, "- Initial Ticket Pool: %d\n", ticket_pool_init);
	fprintf(stdout, "- Current Ticket Pool: %d\n", ticket_pool.pool);
	fprintf(stdout, "- Client Listen Port: %d\n", cl_lstn_port);
	fprintf(stdout, "- Datacenter Broadcast Base Port: %d\n",dc_lstn_port_base);
	fprintf(stdout, "- Datacenter Hostname: %s\n\n", this_dc.hostname);

	//create threads to listen for online/request/release packets
	dc_lstn_thread_ids = (pthread_t *)malloc((dc_addr_count)*sizeof(pthread_t));
	fflush_out_err();
	for(i = 0; i < dc_addr_count; i++)
	{
		//don't listen for a message from self
		if(dc_sys[i].id == this_dc.id)
		{
			continue;
		}
		dc_lstn_args = (dc_lstn_arg_t *)malloc(sizeof(dc_lstn_arg_t));
		dc_lstn_args->src_id = dc_sys[i].id;
		dc_lstn_args->port_base = dc_lstn_port_base;
		fflush_out_err();
		status = pthread_create(&(dc_lstn_thread_ids[i]), NULL, dc_lstn_thread, (void *)dc_lstn_args);
		if(status != 0)
		{
			dc_log(stderr, "%s%d%s (%s) failed to spawn dc_lstn_thread (errno: %s)\n", err_m, fnc_m, 1);
			return 1;
		}
		pthread_mutex_lock(&dc_lstn_lock);
	}

	//wait for user input to connect to other datacenters
	/*fprintf(stdout, "\nPlease press the \"Enter\" key to link with all other online datacenters.\n>>>");
	fgets(stdin_buf, sizeof(stdin_buf), stdin);
	fprintf(stdout, "Now connecting to online datacenters.\n\n");*/
	delay(5);

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
		dc_bcst_args->dst_id = dc_sys[i].id;
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
	cl_lstn_args = (cl_lstn_arg_t *)malloc(sizeof(cl_lstn_arg_t));
	cl_lstn_args->port = cl_lstn_port;
	fflush_out_err();
	status = pthread_create(&cl_lstn_thread_id, NULL, cl_lstn_thread, (void *)cl_lstn_args);
	if(status != 0)
	{
		return dc_log(stderr, "%s%d%s (%s) failed to spawn cl_lstn_thread (errno: %s)\n", err_m, fnc_m, 1);
	}

	dc_log(stdout, "%s%d%s (%s) finished spawning threads\n", log_m, fnc_m, 0);

	//JOIN THREADS
	//SKIP self/FREE CALLS IN BCST WHEN FAIL TO CONNECT(?)

	//join threads once all listening ipc has completed
	for(i = 0; i < dc_addr_count; i++)
	{
		//don't free self
		if(dc_sys[i].id == this_dc.id)
		{
			continue;
		}
		fflush_out_err();
		status = pthread_join(dc_lstn_thread_ids[i], (void **)&dc_lstn_rets);
		fflush_out_err();
		//ret += dc_bcst_rets->ret;
		free(dc_lstn_rets);
	}
	free(dc_lstn_thread_ids);

	//join threads once all broadcast ipc has completed
	for(i = 0; i < dc_addr_count; i++)
	{
		//don't free self
		if(dc_sys[i].id == this_dc.id)
		{
			continue;
		}
		fflush_out_err();
		status = pthread_join(dc_bcst_thread_ids[i], (void **)&dc_bcst_rets);
		fflush_out_err();
		//ret += dc_bcst_rets->ret;
		free(dc_bcst_rets);
	}
	free(dc_bcst_thread_ids);

	fflush_out_err();
	status = pthread_join(cl_lstn_thread_id, (void **)&cl_lstn_rets);
	free(cl_lstn_rets);

	dc_log(stdout, "%s%d%s (%s) finished joining threads\n", log_m, fnc_m, 0);
	dc_log(stdout, "%s%d%s (%s) cleaning up memory and .cfg state\n", log_m, fnc_m, 0);

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

	dc_log(stdout, "%s%d%s (%s) successfully restored .cfg state, now exiting\n", log_m, fnc_m, 0);
	return 0;
}

void dc_init()
{
	pthread_mutex_init(&dc_lstn_lock, NULL);
	pthread_mutex_init(&pool_lock, NULL);
	pthread_mutex_init(&bcst_lock, NULL);
	pthread_mutex_init(&(this_clk.lock), NULL);
	pthread_mutex_lock(&dc_lstn_lock);
	pthread_mutex_lock(&pool_lock);
	pthread_mutex_lock(&bcst_lock);
	this_clk.clk = 0;
	dc_sys_online = 0;
}

int32_t dc_log(FILE *std_strm, char *msg, char *opn_m, char *fnc_m, int32_t errno_f)
{
	pthread_mutex_lock(&(this_clk.lock));
	if(errno_f)
	{
		fprintf(std_strm, msg, opn_m, this_clk.clk, cls_m, fnc_m, strerror(errno));
	}
	else
	{
		fprintf(std_strm, msg, opn_m, this_clk.clk, cls_m, fnc_m);
	}
	pthread_mutex_unlock(&(this_clk.lock));
	fflush_out_err();
	return 1;
}

uint8_t *encode_packet(int32_t p_type, int32_t p_id, int32_t p_clk, int32_t p_pool)
{
	uint8_t *packet_stream = (uint8_t *)malloc(sizeof(packet_t));

	packet_stream[0] = p_type >> 24;
	packet_stream[1] = p_type >> 16;
	packet_stream[2] = p_type >> 8;
	packet_stream[3] = p_type;
	packet_stream[4] = p_id >> 24;
	packet_stream[5] = p_id >> 16;
	packet_stream[6] = p_id >> 8;
	packet_stream[7] = p_id;
	packet_stream[8] = p_clk >> 24;
	packet_stream[9] = p_clk >> 16;
	packet_stream[10] = p_clk >> 8;
	packet_stream[11] = p_clk;
	packet_stream[12] = p_pool >> 24;
	packet_stream[13] = p_pool >> 16;
	packet_stream[14] = p_pool >> 8;
	packet_stream[15] = p_pool;

	return packet_stream;
}

packet_t *decode_packet(uint8_t *packet_stream)
{
	packet_t *packet = (packet_t *)malloc(sizeof(packet_t));

	packet->type = ntohl(*(uint32_t *)packet_stream);
	packet_stream += 4;
	packet->id = ntohl(*(uint32_t *)packet_stream);
	packet_stream += 4;
	packet->clk = ntohl(*(uint32_t *)packet_stream);
	packet_stream += 4;
	packet->pool = ntohl(*(uint32_t *)packet_stream);
	packet_stream += 4;

	return packet;
}

void *dc_lstn_thread(void *args)
{
	int32_t sockopt = 1;
	int32_t status;
	int32_t dc_lstn_sock_fd;
	int32_t dc_rspd_sock_fd;
	int32_t src_id;
	int32_t port_base;
	uint8_t *packet_stream = (uint8_t *)malloc(sizeof(packet_t));
	packet_t *packet;
	char *fnc_m = "dc_lstn_thread";
	struct sockaddr_in dc_lstn_addr;
	struct sockaddr_in dc_rspd_addr;
	socklen_t dc_lstn_addr_len;
	socklen_t dc_rspd_addr_len;
	dc_lstn_arg_t *thread_args = (dc_lstn_arg_t *)args;
	ret_t *thread_rets = (ret_t *)malloc(sizeof(ret_t));

	//unpack args
	fflush_out_err();
	src_id = thread_args->src_id;
	port_base = thread_args->port_base;
	free(thread_args);
	thread_rets->ret = 0;

	//setup the cl_lstn address object
	dc_lstn_addr_len = sizeof(dc_lstn_addr);
	memset((char *)&dc_lstn_addr, 0, dc_lstn_addr_len);
	dc_lstn_addr.sin_family = AF_INET;
	dc_lstn_addr.sin_addr.s_addr = inet_addr(this_dc.hostname);
	dc_lstn_addr.sin_port = htons(port_base + src_id);

	//open a server socket in the datacenter to accept incoming datacenter requests
	status = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
	if(status < 0)
	{
		thread_rets->ret = dc_log(stderr, "%s%d%s (%s) failed to open a socket (errno: %s)\n", err_m, fnc_m, 1);
		pthread_exit((void *)thread_rets);
	}
	dc_lstn_sock_fd = status;
	dc_log(stdout, "%s%d%s (%s) finished initializing a socket for incoming client connections\n", log_m, fnc_m, 0);

	//check if the system is still holding onto the port after a recent restart
	status = setsockopt(dc_lstn_sock_fd, SOL_SOCKET, SO_REUSEADDR, (const void *)&sockopt, sizeof(int));
	if(status < 0)
	{
		thread_rets->ret = dc_log(stderr, "%s%d%s (%s) can't bind to port due to a recent program restart (errno: %s)\n", err_m, fnc_m, 1);
		pthread_exit((void *)thread_rets);
	}
	dc_log(stdout, "%s%d%s (%s) finished setting the socket to be reusable\n", log_m, fnc_m, 0);

	//bind to a port on the datacenter
	status = bind(dc_lstn_sock_fd, (struct sockaddr *)&dc_lstn_addr, dc_lstn_addr_len);
	if(status < 0)
	{
		thread_rets->ret = dc_log(stderr, "%s%d%s (%s) failed to bind to the specified port (errno: %s)\n", err_m, fnc_m, 1);
		pthread_exit((void *)thread_rets);
	}
	dc_log(stdout, "%s%d%s (%s) finished binding to the socket\n", log_m, fnc_m, 0);

	//listen to the port for an incoming initial datacenter connection
	status = listen(dc_lstn_sock_fd, 1);
	if(status < 0)
	{
		thread_rets->ret = dc_log(stderr, "%s%d%s (%s) failed to listen to the port for a datacenter (errno: %s)\n", err_m, fnc_m, 1);
		pthread_exit((void *)thread_rets);
	}
	dc_rspd_addr_len = sizeof(dc_rspd_addr);
	dc_log(stdout, "%s%d%s (%s) now listening for a datacenter connection\n", log_m, fnc_m, 0);
	pthread_mutex_unlock(&dc_lstn_lock);

	//accept connection from datacenter
	status = accept(dc_lstn_sock_fd, (struct sockaddr *)&dc_rspd_addr, &dc_rspd_addr_len);
	if(status < 0)
	{
		thread_rets->ret = dc_log(stderr, "%s%d%s (%s) failed to accept a connection from a datacenter (errno: %s)\n", err_m, fnc_m, 1);
		pthread_exit((void *)thread_rets);
	}
	dc_rspd_sock_fd = status;
	dc_log(stdout, "%s%d%s (%s) accepted a new datacenter connection\n", log_m, fnc_m, 0);

	//recv a packet confirming online datacenter state
	pthread_mutex_lock(&(ticket_pool.lock));
	pthread_mutex_lock(&(this_clk.lock));
	status = recv(dc_rspd_sock_fd, packet_stream, sizeof(packet_t), 0);
	if(status != sizeof(packet_t))
	{
		pthread_mutex_unlock(&(this_clk.lock));
		thread_rets->ret = dc_log(stderr, "%s%d%s (%s) encountered an issue while receving the online packet (errno: %d)\n", err_m, fnc_m, 1);
		pthread_exit((void *)thread_rets);
	}

	//decode packet, make sure signal is for online
	packet = decode_packet(packet_stream);
	if(packet->id != src_id || packet->type != ONLINE)
	{
		pthread_mutex_unlock(&(this_clk.lock));
		thread_rets->ret = dc_log(stderr, "%s%d%s (%s) received an invalid online packet\n", err_m, fnc_m, 0);
		pthread_exit((void *)thread_rets);
	}
	dc_log(stdout, "%s%d%s (%s) received packet: ONLINE\n", log_m, fnc_m, 0);
	free(packet);

	//send the datacenter the online packet
	packet_stream = encode_packet(ACK, src_id, this_clk.clk, ticket_pool.pool);
	status = send(dc_rspd_sock_fd, packet_stream, sizeof(packet_t), 0);
	if(status != sizeof(packet_t))
	{
		pthread_mutex_unlock(&(this_clk.lock));
		thread_rets->ret = dc_log(stderr, "%s%d%s (%s) encountered an issue while sending the ack packet for the online packet (errno: %s)\n", err_m, fnc_m, 1);
		pthread_exit((void *)thread_rets);
	}
	dc_log(stdout, "%s%d%s (%s) sent packet: ACK (ONLINE)\n", log_m, fnc_m, 0);
	free(packet_stream);
	pthread_mutex_lock(&(dc_sys[src_id-1].lock));
	dc_sys[src_id-1].online = 1;
	pthread_mutex_unlock(&(dc_sys[src_id-1].lock));
	pthread_mutex_unlock(&(this_clk.lock));
	pthread_mutex_unlock(&(ticket_pool.lock));

	//continuosly handle request/release messages from another datacenter
	while(1)
	{
		dc_log(stdout, "%s%d%s (%s) begin handling request/release ipc\n", log_m, fnc_m, 0);











	}

	//close the datacenter's socket for incoming client requests
	status = close(dc_lstn_sock_fd);
	if(status < 0)
	{
		thread_rets->ret = dc_log(stderr, "%s%d%s (%s) failed to cleanly close the datacenter's socket for datacenter requests/releases (errno: %s)\n", err_m, fnc_m, 1);
		pthread_exit((void *)thread_rets);
	}
	dc_log(stdout, "%s%d%s (%s) closed the datacenter's socket for datacenter requests/releases\n", log_m, fnc_m, 0);

	fflush_out_err();
	pthread_exit((void *)thread_rets);
}

//send messages to all other datacenters for synchronization
void *dc_bcst_thread(void *args)
{
	int32_t status;
	int32_t dst_id;
	int32_t port_base;
	int32_t dc_bcst_sock_fd;
	uint8_t *packet_stream = (uint8_t *)malloc(sizeof(packet_t));
	char *fnc_m = "dc_bcst_thread";
	packet_t *packet;
	struct sockaddr_in dc_bcst_addr;
	socklen_t dc_bcst_addr_len;
	dc_bcst_arg_t *thread_args = (dc_bcst_arg_t *)args;
	ret_t *thread_rets = (ret_t *)malloc(sizeof(ret_t));
	
	//unpack args
	fflush_out_err();
	dst_id = thread_args->dst_id;
	port_base = thread_args->port_base;
	free(thread_args);
	thread_rets->ret = 0;

	//setup address to connect to the datacenter of dst_id on the port associated with this_dc's id
	dc_bcst_addr_len = sizeof(dc_bcst_addr);
	memset((char *)&dc_bcst_addr, 0, dc_bcst_addr_len);
	dc_bcst_addr.sin_family = AF_INET;
	dc_bcst_addr.sin_addr.s_addr = inet_addr(dc_sys[dst_id-1].hostname);
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
		if(errno == 111)
		{
			thread_rets->ret = dc_log(stdout, "%s%d%s (%s) could not connect to offline datacenter\n", log_m, fnc_m, 0);
		}
		else
		{
			thread_rets->ret = dc_log(stderr, "%s%d%s (%s) failed to connect to the datacenter server (errno: %s)\n", err_m, fnc_m, 1);
		}	
		pthread_mutex_lock(&(dc_sys[dst_id-1].lock));
		dc_sys[dst_id-1].online = 0;
		pthread_mutex_unlock(&(dc_sys[dst_id-1].lock));
		pthread_mutex_unlock(&pool_lock);
		pthread_exit((void *)thread_rets);
	}

	//build a packet signaling this datacenter is now online
	pthread_mutex_lock(&(ticket_pool.lock));
	pthread_mutex_lock(&(this_clk.lock));
	packet_stream = encode_packet(ONLINE, this_dc.id, this_clk.clk, ticket_pool.pool);

	//send the datacenter the online packet
	status = send(dc_bcst_sock_fd, packet_stream, sizeof(packet_t), 0);
	if(status != sizeof(packet_t))
	{
		pthread_mutex_unlock(&(this_clk.lock));
		thread_rets->ret = dc_log(stderr, "%s%d%s (%s) encountered an issue while sending the online packet (errno: %s)\n", err_m, fnc_m, 1);
		pthread_exit((void *)thread_rets);
	}
	free(packet_stream);
	dc_log(stdout, "%s%d%s (%s) sent packet: ONLINE\n", log_m, fnc_m, 0);

	//receive the ack packet back from the server
	status = recv(dc_bcst_sock_fd, packet_stream, sizeof(packet_t), 0);
	if(status != sizeof(packet_t))
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
	dc_log(stdout, "%s%d%s (%s) received packet: ACK (ONLINE)\n", log_m, fnc_m, 0);
	pthread_mutex_lock(&(dc_sys[dst_id-1].lock));
	dc_sys[dst_id-1].online = 1;
	pthread_mutex_unlock(&(dc_sys[dst_id-1].lock));
	pthread_mutex_unlock(&(this_clk.lock));
	pthread_mutex_unlock(&(ticket_pool.lock));
	pthread_mutex_unlock(&pool_lock);

	//handle request/release communication per client transaction
	while(1)
	{
		dc_log(stdout, "%s%d%s (%s) begin handling request/release ipc\n", log_m, fnc_m, 0);
		pthread_mutex_lock(&bcst_lock);

		//build a packet signaling this datacenter is requesting control of the ticket pool
		pthread_mutex_lock(&(ticket_pool.lock));
		pthread_mutex_lock(&(this_clk.lock));
		packet_stream = encode_packet(REQUEST, this_dc.id, this_clk.clk, ticket_pool.pool);

		//send the datacenter the request packet
		status = send(dc_bcst_sock_fd, packet_stream, sizeof(packet_t), 0);
		if(status != sizeof(packet_t))
		{
			pthread_mutex_unlock(&(this_clk.lock));
			thread_rets->ret = dc_log(stderr, "%s%d%s (%s) encountered an issue while sending the request packet (errno: %s)\n", err_m, fnc_m, 1);
			pthread_exit((void *)thread_rets);
		}
		free(packet_stream);
		dc_log(stdout, "%s%d%s (%s) sent packet: REQUEST\n", log_m, fnc_m, 0);

		//receive the ack packet back from the server
		status = recv(dc_bcst_sock_fd, packet_stream, sizeof(packet_t), 0);
		if(status != sizeof(packet_t))
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
		dc_log(stdout, "%s%d%s (%s) received packet: ACK (REQUEST)\n", log_m, fnc_m, 0);
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
		status = send(dc_bcst_sock_fd, packet_stream, sizeof(packet_t), 0);
		if(status != sizeof(packet_t))
		{
			pthread_mutex_unlock(&(this_clk.lock));
			thread_rets->ret = dc_log(stderr, "%s%d%s (%s) encountered an issue while sending the release packet (errno: %s)\n", err_m, fnc_m, 1);
			pthread_exit((void *)thread_rets);
		}
		free(packet_stream);
		dc_log(stdout, "%s%d%s (%s) sent packet: RELEASE\n", log_m, fnc_m, 0);

		//receive the ack packet back from the server
		status = recv(dc_bcst_sock_fd, packet_stream, sizeof(packet_t), 0);
		if(status != sizeof(packet_t))
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
		dc_log(stdout, "%s%d%s (%s) received packet: ACK (RELEASE)\n", log_m, fnc_m, 0);
		pthread_mutex_unlock(&(this_clk.lock));
		pthread_mutex_unlock(&(ticket_pool.lock));
		pthread_mutex_unlock(&pool_lock);
	}

	fflush_out_err();
	pthread_exit((void *)thread_rets);
}

//acts as a frontend for communicating with the client
void *cl_lstn_thread(void *args)
{
	int32_t msg_buf_max = 4096;
	int32_t sockopt = 1;
	int32_t i;
	int32_t status;
	int32_t cl_lstn_sock_fd;
	int32_t cl_rspd_sock_fd;
	int32_t port;
	int32_t ticket_amount;
	char *fnc_m = "cl_lstn_thread";
	char *end;
	char msg_buf[msg_buf_max];
	struct sockaddr_in cl_lstn_addr;
	struct sockaddr_in cl_rspd_addr;
	socklen_t cl_lstn_addr_len;
	socklen_t cl_rspd_addr_len;
	cl_lstn_arg_t *thread_args = (cl_lstn_arg_t *)args;
	ret_t *thread_rets = (ret_t *)malloc(sizeof(ret_t));

	//read out data packed in the args parameter
	fflush_out_err();
	port = thread_args->port;
	free(thread_args);
	thread_rets->ret = 0;

	//setup the cl_lstn address object
	cl_lstn_addr_len = sizeof(cl_lstn_addr);
	memset((char *)&cl_lstn_addr, 0, cl_lstn_addr_len);
	cl_lstn_addr.sin_family = AF_INET;
	cl_lstn_addr.sin_addr.s_addr = inet_addr(this_dc.hostname);
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
		status = accept(cl_lstn_sock_fd, (struct sockaddr *)&cl_rspd_addr, &cl_rspd_addr_len);
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
			sprintf(msg_buf, "%d", 0);
			fprintf(stdout, "%s%d%s (%s) rejected a request from a client for %d ", log_m, this_clk.clk, cls_m, fnc_m, ticket_amount);
			print_tickets(ticket_amount);
			fprintf(stdout, " (%d ", ticket_pool.pool);
			print_tickets(ticket_pool.pool);
			fprintf(stdout, " remaining in the pool)\n");
		}
		else
		{
			ticket_pool.pool -= ticket_amount;
			sprintf(msg_buf, "%d", 1);
			fprintf(stdout, "%s%d%s (%s) accepted a request from a client for %d ", log_m, this_clk.clk, cls_m, fnc_m, ticket_amount);
			print_tickets(ticket_amount);
			fprintf(stdout, " (%d ", ticket_pool.pool);
			print_tickets(ticket_pool.pool);
			fprintf(stdout, " remaining in the pool)\n");
		}
		pthread_mutex_unlock(&this_clk.lock);
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