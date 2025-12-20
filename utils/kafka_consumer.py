#!Execute Tableau API for retrieving data , manipulating and automating tasks
from utils.configuration_error import ConfigurationError
from confluent_kafka import Consumer , KafkaError , KafkaException
from data_steps.flow_manager import FlowManager
import socket
import json
import sys


class ConfluentConsumer():
    def __init__(self, config, logger):
        self.logger = logger
        self.config = config["env_config"]
        self.document_counter = 0
        self.kafka_client_topics = self.config["INPUT_TOPICS"]
        self.group_id = self.config["GROUP_ID"]
        self.bootstrap  = self.config["BOOTSTRAP"]
        self.consumer = self.connect()
        self.fm = FlowManager(config, logger)

    def connect(self):
        try:
            return Consumer(self.sasl_conf())
        except Exception as e:
            raise ConfigurationError(str(e))

    def sasl_conf(self):
        conf = {}
        conf = {'bootstrap.servers': self.bootstrap, 'group.id': self.group_id, 'session.timeout.ms': 6000,
                'auto.offset.reset': 'earliest', 'enable.auto.offset.store': False}
        conf['client.id'] = socket.gethostname()
        return conf

    def consume(self):
        self.basic_consume_loop(self.consumer,self.kafka_client_topics)

    def basic_consume_loop(self, consumer, topics):
        try:
            running = True
            consumer.subscribe(topics)

            while running:
                msg = consumer.poll(timeout=1.0)
                if msg is None: continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                         (msg.topic(), msg.partition(), msg.offset()))
                    elif msg.error():
                        raise KafkaException(msg.error())
                else:
                    self.msg_process(msg)
        finally:
            # Close down consumer to commit final offsets.
            consumer.close()

    def shutdown(self):
        running = False

    def msg_process(self,msg):
        metadata = {
                "topic": msg.topic(),
                "partition": msg.partition(),
                "offset": msg.offset(),
                "timestamp": msg.timestamp()[1],  # msg.timestamp() -> (type, ts)
            }
        dict = json.loads(msg.value())
        dict["metadata"]=metadata
        self.fm.execute_flow(dict,dict)
