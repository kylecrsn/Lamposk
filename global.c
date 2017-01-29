#include "glboal.h"

void delay(uint32_t seconds) 
{
	uint32_t delay_time = time(0) + seconds;
	while (time(0) < delay_time);
}

void lock_cfg(config_t *cf, char *fn)
{
	while(1)
	{
		//intialize the config object
		config_init(cf);

		//check for any errors in the config file
		if(!config_read_file(cf, fn))
		{
			fprintf(stderr, "%s%s:%d - %s\n", err_msg, config_error_file(cf), config_error_line(cf), config_error_text(cf));
			config_destroy(cf);
			return 1;
		}

		//check the current stauts of the file lock
		lock_setting = config_lookup(cf, "lock");
		if(config_setting_get_int(lock_setting) == 1)
		{
			//release cf and try again
			config_destroy(cf);
		}
		else
		{
			//claim the lock and write to file
			config_setting_set_int(lock_setting, 1);
			config_write_file(cf, "global.cfg");
			break;
		}
	}
}
onfig_t cf_local, *cf;
	config_setting_t *lock_setting;