#include "global.h"
#include "datacenter.h"
#include "client.h"

int main (int argc, char *argv[])
{
	int8_t ret = 0;
	int32_t c;
	bool d_flag = false;
	char *usage = "invalid or missing options\nusage: ./ciosk [-d]";

	global_init();

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
		{
			break;
		}

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

	//datacenter
	if(d_flag)
	{
		ret = datacenter_handler();
	}
	//client
	else
	{
		ret = client_handler();
	}

	return ret;
}
