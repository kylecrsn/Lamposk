#include "client.h"

int client_handler()
{
	int status = 0;
	int msg_buf_max = 4096;
	int datacenter_addr_running = 0;
	int i;
	int request_response;
	int client_sock_fd;
	int client_recv_port;
	int datacenter_count;
	int datacenter_id;
	int ticket_count;
	char stdin_buf[256];
	char msg_buf[msg_buf_max];
	char *end;
	char *datacenter_status;
	const char *hostname;
	struct sockaddr_in datacenter_addr;
	socklen_t datacenter_addr_len;
	config_setting_t *datacenter_addr_settings;
	config_setting_t *datacenter_addr_elem_setting;
	datacenter_obj *datacenters;

	fprintf(stdout, "========================\n|   Welcome to Ciosk   |\n========================\n\n");

	//read in datacenter values from locked config file, unlock file, destroy config variable
	config_lookup_int(cf, "datacenter.client_recv_port", &client_recv_port);
	datacenter_addr_settings = config_lookup(cf, "datacenter.addresses");
	datacenter_count = config_setting_length(datacenter_addr_settings);
	datacenters = (datacenter_obj *)malloc(datacenter_count * sizeof(datacenter_obj));
	for(i = 0; i < datacenter_count; i++)
	{
		datacenter_addr_elem_setting = config_setting_get_elem(datacenter_addr_settings, i);
		config_setting_lookup_int(datacenter_addr_elem_setting, "id", &datacenters[i].id);
		config_setting_lookup_int(datacenter_addr_elem_setting, "running", &datacenters[i].running);
		config_setting_lookup_string(datacenter_addr_elem_setting, "hostname", &hostname);
		datacenters[i].hostname = (char *)malloc(strlen(hostname) + 1);
		memcpy(datacenters[i].hostname, hostname, strlen(hostname) + 1);
		if(datacenters[i].running == 1)
		{
			datacenter_addr_running++;
		}
	}
	config_setting_set_int(lock_setting, 0);
	config_write_file(cf, "global.cfg");
	config_destroy(cf);

	//handle if there are no datacenters open
	if(datacenter_addr_running == 0)
	{
		fprintf(stdout, "There are currently no kiosks online, please try again later.\n");
		for(i = 0; i < datacenter_count; i++)
		{
			free(datacenters[i].hostname);
		}
		return 1;
	}

	//ask user to select which data center to connect to
	fprintf(stdout, "There are currently %d kiosks online. Which one would like to connect to?\n", datacenter_addr_running);
	fprintf(stdout, "KIOSK | STATUS\n==============\n");
	for(i = 0; i < datacenter_count; i++)
	{
		if(datacenters[i].running == 1)
		{
			datacenter_status = "ONLINE";
		}
		else
		{
			datacenter_status = "OFFLINE";
		}

		fprintf(stdout, "#%d    | %s\n", datacenters[i].id, datacenter_status);
	}
	fprintf(stdout, "(To select a kiosk type its number)\n\n");
	
	while(1)
	{
		fprintf(stdout, "Kiosk #");
		memset(stdin_buf, 0, sizeof(stdin_buf));

		//read from stdin
		if(!fgets(stdin_buf, sizeof(stdin_buf), stdin))
		{
			fprintf(stderr, "%sclient had an issue occurred while reading from stdin\n", err_msg);
			for(i = 0; i < datacenter_count; i++)
			{
				free(datacenters[i].hostname);
			}
			return 1;
		}

		//validate input from stdin
		errno = 0;
		datacenter_id = strtol(stdin_buf, &end, 10);
		if((*end != 0 && *end != 9 && *end != 10 && *end != 32) || errno != 0 || datacenter_id < 1 
			|| datacenter_id > datacenter_count || datacenters[datacenter_id-1].running == 0)
		{
			fprintf(stdout, "An invalid value was provided while selecting a kiosk, please try again.\n\n");
		}
		else
		{
			fprintf(stdout, "You have selected Kiosk #%d\n\n", datacenter_id);
			break;
		}
	}

	//ask the user to specify the number of tickets they would like to buy
	fprintf(stdout, "How many tickets would you like to buy?\n(maximum purchase allowed: 100 tickets)\n\n");
	
	while(1)
	{
		fprintf(stdout, "Buy ");
		memset(stdin_buf, 0, sizeof(stdin_buf));

		//read from stdin
		if(!fgets(stdin_buf, sizeof(stdin_buf), stdin))
		{
			fprintf(stderr, "%sclient had an issue occurred while reading from stdin\n", err_msg);
			for(i = 0; i < datacenter_count; i++)
			{
				free(datacenters[i].hostname);
			}
			return 1;
		}

		//validate input from stdin
		errno = 0;
		ticket_count = strtol(stdin_buf, &end, 10);
		if((*end != 0 && *end != 9 && *end != 10 && *end != 32) || errno != 0 || ticket_count < 0 || ticket_count > 100)
		{
			fprintf(stdout, "An invalid value was provided while specifying the number of tickets to buy, please try again.\n\n");
		}
		else
		{
			if(ticket_count == 1)
			{
				fprintf(stdout, "You have requested to buy %d ticket.\n\n", ticket_count);
			}
			else
			{
				fprintf(stdout, "You have requested to buy %d tickets\n\n", ticket_count);
			}
			break;
		}
	}
	fprintf(stdout, "Your request is now being sent to Kiosk #%d for processing...\n", datacenter_id);

	RETRY:
	//setup the datacenter address object
	datacenter_addr_len = sizeof(datacenter_addr);
	memset((char *)&datacenter_addr, 0, datacenter_addr_len);
	datacenter_addr.sin_family = AF_INET;
	datacenter_addr.sin_addr.s_addr = inet_addr(datacenters[datacenter_id-1].hostname);
	datacenter_addr.sin_port = htons(client_recv_port);

	//open a client socket
	status = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
	if(status < 0)
	{
		fprintf(stderr, "%sclient failed to open a socket (status: %d/errno: %d)\n", err_msg, status, errno);
		return 1;
	}
	client_sock_fd = status;

	//connect to the datacenter
	status = connect(client_sock_fd, (struct sockaddr*)&datacenter_addr, datacenter_addr_len);
	if(status < 0)
	{
		fprintf(stdout, "Kiosk #%d has gone offline since the time you selected it, please try again later.\n", datacenter_id);
		return 1;
	}

	//send the datacenter the number of tickets requested
	sprintf(msg_buf, "%d", ticket_count);
	delay(msg_delay);
	status = send(client_sock_fd, msg_buf, msg_buf_max, 0);
	if(status == -1 && errno == 104)
	{
		//close the socket connection to the datacenter and try again
		fprintf(stdout, "Kiosk #%d was busy, retrying...\n", datacenter_id);
		status = close(client_sock_fd);
		if(status < 0)
		{
			fprintf(stderr, "%sclient failed to cleanly close the socket (status: %d/errno: %d)\n", err_msg, status, errno);
			return 1;
		}
		goto RETRY;
	}
	if(status != msg_buf_max)
	{
		fprintf(stderr, "%sclient encoutnered an issue while sending request (status: %d/errno: %d)\n", err_msg, status, errno);
		return 1;
	}

	//wait to receive a response from the datacenter
	status = recv(client_sock_fd, msg_buf, msg_buf_max, 0);
	if(status == -1 && errno == 104)
	{
		//close the socket connection to the datacenter and try again
		fprintf(stdout, "Kiosk #%d was busy, retrying...\n", datacenter_id);
		status = close(client_sock_fd);
		if(status < 0)
		{
			fprintf(stderr, "%sclient failed to cleanly close the socket (status: %d/errno: %d)\n", err_msg, status, errno);
			return 1;
		}
		goto RETRY;
	}
	else if(status != msg_buf_max)
	{
		fprintf(stdout, "%sclient encountered an issue while reading the response (status: %d/errno: %d)\n", err_msg, status, errno);
		return 1;
	}

	//convert response to an int
	errno = 0;
	request_response = strtol(msg_buf, &end, 10);
	if(*end != 0 || errno != 0 || request_response < 0 || request_response > 1)
	{
		fprintf(stderr, "%sclient encountered an error while converting request response (errno: %d)", err_msg, errno);
		return 1;
	}

	//close the socket connection to the datacenter
	status = close(client_sock_fd);
	if(status < 0)
	{
		fprintf(stderr, "%sclient failed to cleanly close the socket (status: %d/errno: %d)\n", err_msg, status, errno);
		return 1;
	}

	//tell client result of request
	if(request_response == 1)
	{
		if(ticket_count == 1)
		{
			fprintf(stdout, "Your request to buy %d ticket was accepted.\n\n", ticket_count);
		}
		else
		{
			fprintf(stdout, "Your request to buy %d tickets was accepted.\n\n", ticket_count);
		}		
	}
	else
	{
		if(ticket_count == 1)
		{
			fprintf(stdout, "Your request to buy %d ticket was rejected.\n\n", ticket_count);
		}
		else
		{
			fprintf(stdout, "Your request to buy %d tickets was rejected.\n\n", ticket_count);
		}	
	}

	fprintf(stdout, "Thank you for shopping with Ciosk!\n");

	return 0;
}