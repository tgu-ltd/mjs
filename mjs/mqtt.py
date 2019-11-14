import json
import logging
import traceback
import paho.mqtt.client as mqttc
from mjs.config import Config
from mjs.sql import Sql

''' Things to consider
    See: https://github.com/eclipse/paho.mqtt.python#connect-reconnect-disconnect
'''


class Mqtt():

    def __init__(self, **kwargs):
        '''  A threaded class to listen for mqtt messages '''
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
        self.client.on_message=self.on_message
        self.client.on_connect = self.on_connect

    def on_connect(self, client, userdata, flags, rc):
        ''' Mehthod defined as per paho instructions '''
        self.logger.debug("Connected with result code " + str(rc))

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
        '''  Connect, Subscribe and loop the mqtt connection '''
        logging.debug('Connecting to MQTT server')
        self.client.connect(
            self.config.get('server', '127.0.0.1'),
            keepalive=self.config.get('timeout', 60),
            port=self.config.get('port', 1883),
        )
        self.__log_start()
        self.client.subscribe(self.topics)
        if self.config.get('forever'):
            self.client.loop_forever()
        else:
            self.client.loop_start()

    def disconnect(self):
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
