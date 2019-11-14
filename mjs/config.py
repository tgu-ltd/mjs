import sys
import json
import socket
import logging
import mjs.logger as logger


class Config(object):

    def __init__(self, **kwargs):
        print("Loading config")
        ''' A class to load the internal config options '''
        self.config = {}
        self.host = socket.gethostname()

        # procedually set config options
        self.__load_base_config()
        self.__load_kwargs(**kwargs)
        self.__load_cli()

        # Start the logger for all other objects to log to
        self.logger = logger.start(self.config)
        self.__log_start()

    def __log_start(self):
        cmsg = 'Config setup and logging'
        lmsg = 'Logging to {0}'.format(self.config.get('logfile'))
        jmsg = json.dumps(self.config, sort_keys=True, indent=True)
        if not self.config.get('logtocon', False):
            print(lmsg)
            print(jmsg)
            print(cmsg)
        logging.info(lmsg)
        logging.info(jmsg)
        logging.info(cmsg)

    def get(self):
        ''' Return the config dict '''
        return self.config

    def __load_base_config(self):
        ''' Set the base config '''
        self.config = {
            "port": 1883,
            "topics": "#",
            "timeout": 60,
            "logmode": "w",
            "host": self.host,
            "logtocon": False,
            "loglevel": "WARNING",
            "server": "127.0.0.1",
            "logfile": "./mjs.log",
            "dbfile": "./mjs.db",
            "forever": True,
            "logformat": "%(asctime)-15s %(message)s"
        }

    def __load_kwargs(self, **kwargs):
        ''' Set config options set via intialization '''
        for arg in kwargs:
            if arg in self.config:
                self.config[arg] = kwargs.get(arg)

    def __load_cli(self):
        ''' Overwrite config options passed on the CLI '''
        keys = sys.argv[1::2]
        vals = sys.argv[2::2]
        if 'help' in keys:
            print("")
            print("Available configuration options are ...")
            print('{0}'.format(json.dumps(self.config, indent=True, sort_keys=True)))
            sys.exit(1)

        if len(keys) != len(vals):
            raise Exception("Command line arguments are not in key value pairs")

        for key, value in zip(keys, vals):
            if value.lower() == 'true':
                self.config[key] = True
            elif value.lower() == 'false':
                self.config[key] = False
            else:
                try:
                    self.config[key] = int(value)   # Try and make it a int
                except ValueError as e:
                    try:
                        self.config[key] = float(value)  # Try and make it a float
                    except ValueError as e:
                        if 'loglevel' in key:
                            value = value.upper()
                        self.config[key] = value   # default string
                        pass
