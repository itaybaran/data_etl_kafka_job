import datetime
import copy


class StepError(Exception):
    def __init__(self, description,order):
        super().__init__()
        self.msg = description
        self.order = order

class BaseStep:
    def __init__(self,config,logger,step_order,raise_event):
        self.config = config
        self.logger = logger
        self.msg = []
        self.current_messages = []
        self.order = int(step_order)
        self.step_config = self.get_step_config()
        self.name = self.step_config["class"]

    @property
    def order(self):
        # Getter
        return self._order

    @order.setter
    def order(self, value):
        # Setter with validation
        self._order = value

    def get_step_config(self):
        for key in self.config["steps"]:
            if key["order"] == self.order:
                return key

    def execute(self,msg,payload):
        self.current_messages = []
        self.msg = msg
        if isinstance(msg,list):
            for message in msg:
                res = self.executer(message,payload)
        else:
            res = self.executer(msg,payload)
        self.msg = self.current_messages
        return res

    def executer(self,message,payload):
        pass
        
    def pre_execute(self,message,payload):
        pass
    
    def _find_key(self, dict, key_str, seperator):
        keys_arr =str(key_str).split(sep=seperator)
        for key in keys_arr:
            dict = dict[key]
        return dict
    
    def filter(self,msg):
        filter_key = self.config["filters_key_in_message"]
        if filter_key in msg:
            filter = msg[filter_key]
        else:
            filter = "all"
            msg[filter_key]=filter
        return  filter in self.step_config["flow_filter"]

    