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
        self.filters = self.step_config["filter_flow"]
        self.name = self.step_config["class"]
        self.raise_event = raise_event

    def raise_event(self): 
        pass

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

    