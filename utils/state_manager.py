import json
from utils.configuration_error import ConfigurationError
import redis
import copy


class StateError(Exception):
    def __init__(self, description):
        super().__init__()
        self.msg = description


class StateManager():
    def __init__(self,config,env_config,logger):
        self.main_config = config
        self.logger = logger
        self.entity_name = ""
        self.key = None
        self.seperator = ""
        self.root_message = {}
        self.current_message = {}
        self.state = redis.Redis(
            host=env_config["REDIS_HOST"],     # container name on devnet
            port=env_config["REDIS_PORT"],
            password=env_config["REDIS_PWD"],  # same as in podman run
            decode_responses=True,
        )

        
    def bind_entity(self,message):
        res = False
        try:
            self.save_key(message)
            topic = message["metadata"]["topic"]
            self.seperator = self.main_config["seperator"]
            parts = list(filter(lambda part: part["topic"]  == topic, self.main_config["parts"]))
            part = parts[0]
            level = part["bind_info"]["level"]
            if level==1:
                # check and fill predecessors
                self.root_message = message
                bind_ready=True
            elif level>1:
                self.combine_message(message=message,direction=1,flag=True)
                bind_ready = self.root_message != {}
            if bind_ready:
                self.combine_message(message=self.root_message,direction=-1,flag=True)
                bind_ready = self.is_bind_ready()
            return bind_ready

        except StateError as e:
            self.logger.insert_error_to_log(-301,"State error in set method, issue:{}".format(str(e)))
        except Exception as e:
            self.logger.insert_error_to_log(-301,"State error in set method, issue:{}".format(str(e)))

    def save_key(self,message):
        res = True
        try:
            topic = message["metadata"]["topic"]
            self.seperator = self.main_config["seperator"]
            parts = list(filter(lambda part: part["topic"]  == topic, self.main_config["parts"]))
            part = parts[0]
            key = part["key_field"]
            key = self.find_key(message,key,self.seperator)
            parent_key = part["bind_info"]["key_field"]
            parent_key = self.find_key(message,parent_key,self.seperator)
            fragments = ["None"] * 3
            fragments[0] = topic
            fragments[1] = parent_key
            fragments[2] = key
            key_patern = ":".join(fragments)
            self.state.set(key_patern,json.dumps(message))
        except StateError as e:
            res = False
            self.logger.insert_error_to_log(-301,"State error in save_key method, issue:{}".format(str(e)))
        except Exception as e:
            res = False
            self.logger.insert_error_to_log(-301,"State error in save_key method, issue:{}".format(str(e)))
        finally:
            return res

    def is_bind_ready(self):
        try:
            for part in self.main_config["parts"]:
                if not isinstance(self.current_message[part["name"]],dict):
                    return False
            return True
        except StateError as e:
            self.logger.insert_error_to_log(-301,"State error in is_bind_ready method, issue:{}".format(str(e)))
        except Exception as e:
            self.logger.insert_error_to_log(-301,"State error in is_bind_ready method, issue:{}".format(str(e)))  

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

    def get_patern(self,fragments):
        res = None
        try:
            res = ":".join(fragments)
            res = res.replace("None","*")
            return res
        except StateError as e:
            self.logger.insert_error_to_log(-301,"State error in get_patern method, issue:{}".format(str(e)))
        except Exception as e:
            self.logger.insert_error_to_log(-301,"State error in get_patern method, issue:{}".format(str(e)))

    def find_related_topic(self,part,direction):
        res = None
        try:
            if direction==-1:# direction=1 will look for parent node, direction=-1 will look for child node
                name = part["name"]
                parts = list(filter(lambda part: part["bind_info"]["parent_name"]  == name, self.main_config["parts"]))
            elif direction==1:# direction=1 will look for parent node, direction=-1 will look for child node
                name = part["bind_info"]["parent_name"]
                parts = list(filter(lambda part: part["name"]  == name, self.main_config["parts"]))
            return parts
        except StateError as e:
            self.logger.insert_error_to_log(-301,"State error in find_related_topic method, issue:{}".format(str(e)))
        except Exception as e:
            self.logger.insert_error_to_log(-301,"State error in find_related_topic method, issue:{}".format(str(e)))

    def combine_message(self,message,direction,flag):
        # direction=1 will look for parent node, direction=-1 will look for child node
        res = None
        self.key = None
        self.entity_name = ""
        self.seperator = ""
        try:
            topic = message["metadata"]["topic"]
            self.seperator = self.main_config["seperator"]
            parts = list(filter(lambda part: part["topic"]  == topic, self.main_config["parts"]))
            part = parts[0]
            name =  part["name"]
            level = part["bind_info"]["level"]
            self.current_message[name] = message
            if flag:
                if level == 1 and direction==1:
                    self.root_message = message
                    self.combine_message(message=message,direction=direction,flag=False)
                key = part["key_field"]
                key = self.find_key(message,key,self.seperator)
                parent_key = part["bind_info"]["key_field"]
                parent_key = self.find_key(message,parent_key,self.seperator)
                parts= self.find_related_topic(part=part,direction=direction)
                for part in parts:
                    level = part["bind_info"]["level"]
                    fragments = ["None"] * 3
                    fragments[0] = part["topic"]
                    if direction == -1: # look for child node
                        fragments[1] = key
                        fragments[2] = "*"
                    elif direction == 1: # look for parent node
                        fragments[1] = "*"
                        fragments[2] = parent_key
                    search_patern = self.get_patern(fragments=fragments)
                    cur_flag = False # set flag to Flase in case there are no needed relatives
                    for key_exists in self.state.scan_iter(match=search_patern, count=100):
                        cur_flag = True # set flag to True after finding relatives
                        entity = self.state.get(key_exists)
                        message = json.loads(entity)
                        self.combine_message(message=message,direction=direction,flag=flag)
                    if not cur_flag:
                        self.combine_message(message=message,direction=direction,flag=False)

        except StateError as e:
            self.logger.insert_error_to_log(-301,"State error in combine_message method, issue:{}".format(str(e)))
            return res
        except Exception as e:
            self.logger.insert_error_to_log(-301,"State error in combine_message method, issue:{}".format(str(e)))
            return res