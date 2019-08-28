import json
import time
import logging
import traceback
from threading import Thread
import paho.mqtt.client as mqttc
from mjs.sql import Sql
from mjs.config import Config

''' Things to consider
    See: https://github.com/eclipse/paho.mqtt.python#connect-reconnect-disconnect
'''


class Mqtt(Thread):

    def __init__(self, **kwargs):
        '''  A threaded class to listen for mqtt messages '''
        Thread.__init__(self)
        self.running = True
        self.topics = []

        # CLI args will overide any kwargs
        # Setup config with passed config options
        self.config = Config(**kwargs).get()
        # Thats it, it is set, no going back now

        # Start all other objects
        self.logger = logging.getLogger()
        self.database = Sql(config=self.config)

        # Setup topics to listen too, default is '#'=all
        topics = self.config.get('topics', '#')
        if ',' in topics:
            for topic in topics.split(','):
                # Topic name and QOS
                self.topics.append((topic, 0))
        else:
            self.topics = topics

        # Setup and start the mqtt client
        self.client=mqttc.Client()
        self.client.connect(
            self.config.get('server', '127.0.0.1'),
            self.config.get('port', '1823'),
            self.config.get('timeout', 60)
        )
        self.__log_start()
        self.client.loop_start()

    def __log_start(self):
        ''' Log the start of the mqtt listener '''
        msg = "Loading mqtt server {0} port {1} topics {2} ".format(
            self.config.get('server', '127.0.0.1'),
            self.config.get('port', '1823'),
            self.topics
        )
        self.logger.info(msg)
        if not self.config.get('logtocon', False):
            print(msg)

    def connect(self):
        logging.debug('Connecting to MQTT server')
        self.client.on_message=self.on_message
        self.client.subscribe(self.topics)
        return self

    def stop(self):
        self.running = False
        self.client.loop_stop()
        self.client.disconnect()

    def on_message(self, client, userdata, message):
        data = json.loads(str(message.payload.decode("utf-8")))
        try:
            with self.database:
                self.logger.debug("New {0} message".format(message.topic))
                self.database.save(message.topic, data)
        except Exception as e:
            self.logger.error(traceback.print_exc())
            self.logger.error(e)
            pass
