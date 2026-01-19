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
        self.incoming_message = {}
        self.current_message = {}
        self.messages_2_produce=[]
        self.state = redis.Redis(
            host=env_config["REDIS_HOST"],     # container name on devnet
            port=env_config["REDIS_PORT"],
            password=env_config["REDIS_PWD"],  # same as in podman run
            decode_responses=True,
        )

        
    def bind_entity(self,message):
        res = False
        try:
            self.incoming_message = message
            self.current_message = {}
            self.root_message = {}
            self.messages_2_produce=[]
            self.save_key(message)
            sub_entity_id = message["sub_entity_id"]
            self.seperator = self.main_config["seperator"]
            parts = list(filter(lambda part: part["sub_entity_id"]  == sub_entity_id, self.main_config["parts"]))
            part = parts[0]
            level = part["bind_info"]["level"]
            if level==1:
                # check and fill predecessors
                self.root_message = message
                bind_ready=True
            elif level>1:
                self.combine_message(message=message,direction=1,recurse_level=1,flag=True)
                bind_ready = self.root_message != {}
            if bind_ready:
                self.combine_message(message=self.root_message,direction=-1,recurse_level=1,flag=True)
                bind_ready = self.is_bind_ready()
                if bind_ready:
                    self.tag_sent_messages()
            return bind_ready

        except StateError as e:
            self.logger.insert_error_to_log(-301,"State error in set method, issue:{}".format(str(e)))
        except Exception as e:
            self.logger.insert_error_to_log(-301,"State error in set method, issue:{}".format(str(e)))

    def save_key(self,message):
        res = True
        try:
            sub_entity_id = message["sub_entity_id"]
            self.seperator = self.main_config["seperator"]
            parts = list(filter(lambda part: part["sub_entity_id"]  == sub_entity_id, self.main_config["parts"]))
            part = parts[0]
            key = part["key_field"]
            key = self.find_key(message,key,self.seperator)
            parent_key = part["bind_info"]["parent_key_field"]
            parent_key = self.find_key(message,parent_key,self.seperator)
            fragments = ["None"] * 3
            fragments[0] = sub_entity_id
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
                # all sub entities must exist
                if not part["name"] in self.current_message:
                    return False
            return True
        except StateError as e:
            self.logger.insert_error_to_log(-301,"State error in is_bind_ready method, issue:{}".format(str(e)))
        except Exception as e:
            self.logger.insert_error_to_log(-301,"State error in is_bind_ready method, issue:{}".format(str(e))) 

    def tag_sent_messages(self):
        try:
            parts = list(filter(lambda part: part["tag_after_sent"], self.main_config["parts"]))
            for part in parts:
                msg = self.current_message[part["name"]]
                msg["metadata.sent"] = True
                self.save_key(msg)
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

    def find_related_part(self,part,direction):
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
            self.logger.insert_error_to_log(-301,"State error in find_related_part method, issue:{}".format(str(e)))
        except Exception as e:
            self.logger.insert_error_to_log(-301,"State error in find_related_part method, issue:{}".format(str(e)))

    def save_4_produce(self,part,message,bind_message):
        res = None
        try:
            # collect all messages the need to be send in case the leaf messages arived before parent mesage
            # in this case more than one messages can be send
            # will not send again if the message is a leaf object that allready been sent (tag_after_sent = False)
            if "tag_after_sent" in part:
                if part["tag_after_sent"]:
                    if not message["metadata.sent"]:
                        current_message = copy.copy(bind_message)
                        self.messages_2_produce.append(current_message)
        except StateError as e:
            self.logger.insert_error_to_log(-301,"State error in find_related_part method, issue:{}".format(str(e)))
        except Exception as e:
            self.logger.insert_error_to_log(-301,"State error in find_related_part method, issue:{}".format(str(e)))

    def combine_message(self,message,direction,recurse_level,flag):
        # direction=1 will look for parent node, direction=-1 will look for child node
        res = None
        self.key = None
        self.entity_name = ""
        self.seperator = ""
        try:
            sub_entity_id = message["sub_entity_id"]
            self.seperator = self.main_config["seperator"]
            parts = list(filter(lambda part: part["sub_entity_id"]  == sub_entity_id, self.main_config["parts"]))
            part = parts[0]
            name =  part["name"]
            level = part["bind_info"]["level"]
            self.logger.insert_debug_to_log("combine_message","incoming message sub_entity_id:{}, recurse level:{}".format(part["sub_entity_id"],recurse_level))
            incoming_sub_entity_id = self.incoming_message["sub_entity_id"]
            if incoming_sub_entity_id==sub_entity_id:
                self.current_message[name] = self.incoming_message
            else:
                self.current_message[name] = message
            self.save_4_produce(part=part,message=message,bind_message=self.current_message)

            if flag:
                if level == 1 and direction==1:
                    self.root_message = message
                    self.combine_message(message=message,direction=direction,recurse_level=recurse_level+1,flag=False)
                key = part["key_field"]
                key = self.find_key(message,key,self.seperator)
                parent_key = part["bind_info"]["parent_key_field"]
                parent_key = self.find_key(message,parent_key,self.seperator)
                parts= self.find_related_part(part=part,direction=direction)
                for part in parts:
                    level = part["bind_info"]["level"]
                    fragments = ["None"] * 3
                    fragments[0] = part["sub_entity_id"]
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
                        self.combine_message(message=message,direction=direction,recurse_level=recurse_level+1,flag=flag)
                    if not cur_flag:
                        self.combine_message(message=message,direction=direction,recurse_level=recurse_level+1,flag=False)


        except StateError as e:
            self.logger.insert_error_to_log(-301,"State error in combine_message method, issue:{}".format(str(e)))
            return res
        except Exception as e:
            self.logger.insert_error_to_log(-301,"State error in combine_message method, issue:{}".format(str(e)))
            return res