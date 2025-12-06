#!Execute Tableau API for retrieving data , manipulating and automating tasks
from utils.configuration_error import ConfigurationError
from confluent_kafka import Consumer , KafkaError , KafkaException
import socket
import json
import sys


class ConfluentConsumer():
    def __init__(self, config, logger):
        try:
            self.logger = logger
            self.config = config["env_config"]
            self.document_counter = 0
            self.kafka_client_topics = self.config["INPUT_TOPICS"]
            self.group_id = self.config["GROUP_ID"]
            self.bootstrap  = self.config["BOOTSTRAP"]
            self.consumer = self.connect()
        except Exception as e:
            err_dict = {}
            err_dict["error_code"] = ""
            err_dict["error_type"] = ""
            err_dict["error_msg"] = str(e)
            self.logger.logger.error("{}".format(json.dumps(err_dict, sort_keys=True)))

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
        self.redis(json.dumps(dict))
        self.logger.logger.info("{}".format(dict))


    
    def redis(self,msg):
        import redis
        r = redis.Redis(
            host="redis",     # container name on devnet
            port=6379,
            password="ChangeMe123!",  # same as in podman run
            decode_responses=True,
        )

        r.set("msg",msg)
        print(r.get("msg"))