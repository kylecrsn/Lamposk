#include "client.h"

int32_t cl_handler()
{
	int32_t status = 0;
	int32_t msg_buf_max = 4096;
	int32_t dc_addr_online = 0;
	int32_t retry_count = 0;
	int32_t i;
	int32_t fd;
	int32_t rspn;
	int32_t cl_sock_fd;
	int32_t cl_lstn_port;
	int32_t dc_addr_count;
	int32_t i_dc;
	int32_t ticket_count;
	char *fnc_m = "cl_handler";
	char stdin_buf[256];
	char msg_buf[msg_buf_max];
	char *end;
	char *dc_status;
	const char *hostname;
	struct flock *fl;
	struct sockaddr_in dc_addr;
	socklen_t dc_addr_len;
	config_t cf_local;
	config_t *cf;
	config_setting_t *dc_addr_settings;
	config_setting_t *dc_addr_elem_setting;
	dc_t *dc_sys;

	//open and lock the config file
	fd = open(cfg_fn, O_RDWR);
	fl = lock_cfg(fd);
	if(fl == NULL)
	{
		return 1;
	}

	//initialize the config object
	cf = &cf_local;
	config_init(cf);
	if(!config_read_file(cf, cfg_fn))
	{
		fprintf(stderr, "%sn/a%s (%s) encountered an issue while reading from config file: %s:%d - %s\n", 
			err_m, cls_m, fnc_m, config_error_file(cf), config_error_line(cf), config_error_text(cf));
		config_destroy(cf);
		unlock_cfg(fd, fl);
		close(fd);
		return 1;
	}

	//get general config values
	config_lookup_int(cf, "dc.cl_lstn_port", &cl_lstn_port);
	dc_addr_settings = config_lookup(cf, "dc.addrs");
	dc_addr_count = config_setting_length(dc_addr_settings);

	//populate an array with all of the datacenter configs
	dc_sys = (dc_t *)malloc(dc_addr_count * sizeof(dc_t));
	for(i = 0; i < dc_addr_count; i++)
	{
		dc_addr_elem_setting = config_setting_get_elem(dc_addr_settings, i);
		config_setting_lookup_int(dc_addr_elem_setting, "id", &(dc_sys[i].id));
		config_setting_lookup_int(dc_addr_elem_setting, "online", &(dc_sys[i].online));
		config_setting_lookup_string(dc_addr_elem_setting, "hostname", &hostname);
		dc_sys[i].hostname = (char *)malloc(strlen(hostname) + 1);
		memcpy(dc_sys[i].hostname, hostname, strlen(hostname) + 1);

		//set the index for the first availble datacenter not currently online
		if(dc_sys[i].online == 1)
		{
			dc_addr_online++;
		}
	}

	//close and unlock the config file
	config_destroy(cf);
	unlock_cfg(fd, fl);
	close(fd);

	fprintf(stdout, "========================\n|   Welcome to Ciosk   |\n========================\n\n");

	//handle if there are no datacenters open
	if(dc_addr_online == 0)
	{
		fprintf(stdout, "There are currently no kiosks online, please try again later.\n\n");
		return free_dc_sys(dc_sys, dc_addr_count);
	}

	//ask user to select which data center to connect to
	fprintf(stdout, "There are currently %d kiosks online. Which one would like to connect to?\n", 
		dc_addr_online);
	fprintf(stdout, "KIOSK | STATUS\n==============\n");
	for(i = 0; i < dc_addr_count; i++)
	{
		if(dc_sys[i].online == 1)
		{
			dc_status = "ONLINE";
		}
		else
		{
			dc_status = "OFFLINE";
		}

		fprintf(stdout, "#%d    | %s\n", dc_sys[i].id, dc_status);
	}
	fprintf(stdout, "(To select a kiosk type its number)\n\n");
	
	while(1)
	{
		fprintf(stdout, "Kiosk #");
		memset(stdin_buf, 0, sizeof(stdin_buf));

		//read from stdin
		if(!fgets(stdin_buf, sizeof(stdin_buf), stdin))
		{
			fprintf(stderr, "%sn/a%s (%s) had an issue occurred while reading from stdin\n", 
				err_m, cls_m, fnc_m);
			return free_dc_sys(dc_sys, dc_addr_count);
		}

		//validate input from stdin
		errno = 0;
		i_dc = strtol(stdin_buf, &end, 10);
		if((*end != 0 && *end != 9 && *end != 10 && *end != 32) || errno != 0 || i_dc < 1 || i_dc > dc_addr_count)
		{
			fprintf(stdout, "An invalid value was provided while selecting a kiosk, please try again.\n\n");
		}
		else if(dc_sys[i_dc-1].online == 0)
		{
			fprintf(stdout, "The selected kiosk is currently offline, please try again.\n\n");
		}
		else
		{
			fprintf(stdout, "You have selected Kiosk #%d\n\n", i_dc);
			break;
		}
	}

	//ask the user to specify the number of tickets they would like to buy
	fprintf(stdout, "How many tickets would you like to buy?\n");
	
	while(1)
	{
		fprintf(stdout, "Buy ");
		memset(stdin_buf, 0, sizeof(stdin_buf));

		//read from stdin
		if(!fgets(stdin_buf, sizeof(stdin_buf), stdin))
		{
			fprintf(stderr, "%sn/a%s (%s) had an issue occurred while reading from stdin\n", 
				err_m, cls_m, fnc_m);
			return free_dc_sys(dc_sys, dc_addr_count);
		}

		//convert to an int
		errno = 0;
		ticket_count = strtol(stdin_buf, &end, 10);
		if((*end != 0 && *end != 9 && *end != 10 && *end != 32) || errno != 0 || ticket_count < 0)
		{
			fprintf(stdout, "An invalid value was provided while specifying the number of tickets to buy, please try again.\n\n");
		}
		else
		{
			fprintf(stdout, "You have requested to buy %d ", ticket_count);
			print_tickets(ticket_count);
			fprintf(stdout, ".\n\n");
			break;
		}
	}

	fprintf(stdout, "Your request is now being sent to Kiosk #%d.\n", i_dc);

	//setup the datacenter address object
	dc_addr_len = sizeof(dc_addr);
	memset((char *)&dc_addr, 0, dc_addr_len);
	dc_addr.sin_family = AF_INET;
	dc_addr.sin_addr.s_addr = inet_addr(dc_sys[i_dc-1].hostname);
	dc_addr.sin_port = htons(cl_lstn_port);

	//attempt to connect to the datacenter, retying if it is busy
	while(1)
	{
		//open a client socket
		status = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
		if(status < 0)
		{
			fprintf(stderr, "%sn/a%s (%s) failed to open a socket (errno: %s)\n", 
					err_m, cls_m, fnc_m, strerror(errno));
			return free_dc_sys(dc_sys, dc_addr_count);
		}
		cl_sock_fd = status;

		//connect to the datacenter
		status = connect(cl_sock_fd, (struct sockaddr*)&dc_addr, dc_addr_len);
		if(status < 0)
		{
			fprintf(stdout, "Kiosk #%d has gone offline since the time you selected it, please try again later.\n", i_dc);
			return free_dc_sys(dc_sys, dc_addr_count);
		}

		//send the datacenter the number of tickets requested
		memset(msg_buf, 0, msg_buf_max);
		sprintf(msg_buf, "%d", ticket_count);
		status = send(cl_sock_fd, msg_buf, msg_buf_max, 0);
		if(status == -1 && errno == 104)
		{
			//close the socket connection to the datacenter and try again
			if(retry_count == 0)
			{
				fprintf(stdout, "Kiosk #%d was busy serving another client, retrying.", i_dc);
			}
			else
			{
				fprintf(stdout, ".");	
			}
			retry_count++;
			status = close(cl_sock_fd);
			if(status < 0)
			{
				fprintf(stderr, "%sn/a%s (%s) failed to cleanly close the socket (errno: %s)\n", 
					err_m, cls_m, fnc_m, strerror(errno));
				return free_dc_sys(dc_sys, dc_addr_count);
			}
			continue;
		}
		else if(status != msg_buf_max)
		{
			fprintf(stderr, "%sn/a%s (%s) encoutnered an issue while sending request (status: %d/errno: %s)\n", 
				err_m, cls_m, fnc_m, status, strerror(errno));
			return free_dc_sys(dc_sys, dc_addr_count);
		}
		break;
	}
	fprintf(stdout, "Kiosk #%d has received your request, please wait for processing to complete...\n\n", i_dc);

	//wait to receive a response from the datacenter
	memset(msg_buf, 0, msg_buf_max);
	status = recv(cl_sock_fd, msg_buf, msg_buf_max, 0);
	if(status != msg_buf_max)
	{
		fprintf(stderr, "%sn/a%s (%s) encoutnered an issue while receiving the response (status: %d/errno: %s)\n", 
				err_m, cls_m, fnc_m, status, strerror(errno));
		return free_dc_sys(dc_sys, dc_addr_count);
	}
	fprintf(stdout, "Processing complete.\n\n");

	//convert response to an int
	errno = 0;
	rspn = strtol(msg_buf, &end, 10);
	if(*end != 0 || errno != 0 || rspn < 0 || rspn > 1)
	{
		fprintf(stderr, "%sn/a%s (%s) encoutnered an issue while converting the response (errno: %s)\n", 
				err_m, cls_m, fnc_m, strerror(errno));
		return free_dc_sys(dc_sys, dc_addr_count);
	}

	//close the socket connection to the datacenter
	status = close(cl_sock_fd);
	if(status < 0)
	{
		fprintf(stderr, "%sn/a%s (%s) failed to cleanly close the socket (errno: %s)\n", 
					err_m, cls_m, fnc_m, strerror(errno));
		return free_dc_sys(dc_sys, dc_addr_count);
	}

	//tell client the response
	if(rspn == 1)
	{
		fprintf(stdout, "Your request to buy %d ", ticket_count);
		print_tickets(ticket_count);
		fprintf(stdout, " was accepted.\n\n");		
	}
	else
	{
		fprintf(stdout, "Your request to buy %d ", ticket_count);
		print_tickets(ticket_count);
		fprintf(stdout, " was rejected.\n\n");
	}

	fprintf(stdout, "Thank you for shopping with Ciosk!\n");

	return 0;
}

//free heap memory allocated to the datacenters
int32_t free_dc_sys(dc_t *dc_sys, int32_t dc_addr_count)
{
	int32_t i;

	for(i = 0; i < dc_addr_count; i++)
	{
		free(dc_sys[i].hostname);
	}
	free(dc_sys);

	return 1;
}