import json
from utils.configuration_error import ConfigurationError
import redis


class StateError(Exception):
    def __init__(self, description):
        super().__init__()
        self.msg = description


class StateManager():
    def __init__(self,config,logger):
        self.main_config = config["main_config"]
        self.env_config = config["env_config"]
        self.logger = logger
        self.entity_name = ""
        self.key = None
        self.seperator = self.main_config["flink_steps"][0]["seperator"]
        self.state = redis.Redis(
            host=self.env_config["REDIS_HOST"],     # container name on devnet
            port=self.env_config["REDIS_PORT"],
            password=self.env_config["REDIS_PWD"],  # same as in podman run
            decode_responses=True,
        )

    def get(self,message):
        res = None
        try:
            key = self.get_key(message)
            res= self.state.get(key)
            if res is None:
                dict = {}
                dict[self.entity_name] = message
                res = json.dumps(dict)

        except StateError as e:
            self.logger.insert_error_to_log(-301,"State error in get method, issue:{}".format(str(e)))
        except Exception as e:
           self.logger.insert_error_to_log(-301,"State error in get method, issue:{}".format(str(e)))
        finally:
             return res
        
    def set(self,message):
        res = None
        try:
            entity = self.get(message)
            if not entity is None:
                dict = json.loads(entity)
                dict[self.entity_name] = message
                res = self.state.set(self.key,json.dumps(dict))
                #res = self.state.delete(self.key)
                return self.key
        except StateError as e:
            self.logger.insert_error_to_log(-301,"State error in set method, issue:{}".format(str(e)))
        except Exception as e:
            self.logger.insert_error_to_log(-301,"State error in set method, issue:{}".format(str(e)))

        
    def get_key(self,message):
        res = None
        self.key = None
        self.entity_name = ""
        try:
            topic = message["metadata"]["topic"]
            for part in self.main_config["flink_steps"][0]["parts"]:
                if topic == part["topic"]:
                    key_patern = part["key_field"]
                    key = self.find_key(message,key_patern,self.seperator)
                    self.entity_name = part["name"]
                    self.key = "{}|{}".format(part["topic"],key)
                    return self.key
            if res is None:
                raise StateError("Could not find key")
        except StateError as e:
            self.logger.insert_error_to_log(-301,"State error in get_key method, issue:{}".format(str(e)))
            return res
        except Exception as e:
            self.logger.insert_error_to_log(-301,"State error in get_key method, issue:{}".format(str(e)))
            return res

        
    def find_key(self, dict, key_str, seperator):
        res = None
        try:
            keys_arr =str(key_str).split(sep=seperator)
            for key in keys_arr:
                dict = dict[key]
            return dict
        except StateError as e:
            self.logger.insert_error_to_log(-301,"State error in find_key method, issue:{}".format(str(e)))
        except Exception as e:
            self.logger.insert_error_to_log(-301,"State error in find_key method, issue:{}".format(str(e)))
