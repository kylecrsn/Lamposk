#include "global.h"
#include "datacenter.h"
#include "client.h"

int main (int argc, char *argv[])
{
	opterr = 0;
	int8_t ret = 0;
	bool d_flag = false;
	int32_t c;
	err_msg = "<internal error>: ";
	log_msg = "<log>: ";
	char *usage = "invalid or missing options\nusage: ./ciosk [-d]";
	config_t cf_local, *cf;
	config_setting_t *lock_setting;

	//begin command-line parsing
	while(1)
	{
		//setup the options array
		static struct option long_options[] = 
			{
				{"datacenter",	no_argument,	0,	'd'},
				{0, 0, 0, 0}
			};

		//initialize the index and c
		int option_index = 0;
		c = getopt_long(argc, argv, "d", long_options, &option_index);

		//make sure the end hadn't been reached
		if(c == -1)
			break;

		//cycle through the arguments
		switch(c)
		{
			case 'd':
			{
				d_flag = true;
				break;
			}
			case '?':
			{
				ret = 1;
				break;
			}
			default:
			{
				ret = 1;
				break;
			}
		}
	}

	//post-parsing error handling
	if(ret == 1 || optind != argc)
	{
		fprintf(stderr, "%s\n", usage);
		return 1;
	}

	//check if the config file is locked, indicating either a client or server is in the middle of initializing (not idling)
	while(1)
	{
		cf = &cf_local;
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
			break;
		}
	}

	//lookup message delay value
	config_lookup_int(cf, "delay", &msg_delay);

	//datacenter
	if(d_flag == 1)
	{
		ret = datacenter_handler(cf, lock_setting, msg_delay);
	}
	//client
	else
	{
		ret = client_handler(cf, lock_setting, msg_delay);
	}

	return ret;
}
