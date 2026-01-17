#from utils.state_manager import StateManager
from data_steps.parsing import Parsing 
from data_steps.hl7_parser import HL7Parser
from data_steps.validator import Validator
from data_steps.filter import Filter
from data_steps.enrich import Enrich
from data_steps.explode import Explode
from data_steps.add_attributes import AddAttributes
from data_steps.bind import Bind
from data_steps.produce import Produce
from data_steps.flow_filter import FlowFilter

class FlowManagerError:
    def __init__(self, description):
        super().__init__()
        self.msg = description

class FlowManager:
    def __init__(self,config, logger):
        self.config = config["main_config"]
        self.env_config = config["env_config"]
        self.logger = logger
        self.steps = []
        self.create_flow()
        self._msg = None

    @property
    def msg(self):
        # Getter
        return self._msg

    @msg.setter
    def msg(self, value):
        # Setter with validation
        self._msg = value
    
    def create_flow(self):
        steps_config = self.config["flow_manager"]
        steps_config.sort(key=lambda x: x["order"])
        for step_conf in steps_config:
            if step_conf["is_active"]:
                step = self.create_step(step_conf["name"],step_conf["order"])
                step.order = step_conf["order"]
                self.steps.append(step)

    def create_step(self,step_name,step_order):
        if str(step_name).lower() == "hl7_parser":
            return HL7Parser(config=self.config, logger=self.logger,step_order=step_order,raise_event=self.raise_event)
        elif str(step_name).lower() == "parsing":
            return Parsing(config=self.config, logger=self.logger,step_order=step_order,raise_event=self.raise_event)
        elif str(step_name).lower() == "validator":
            return Validator(config=self.config, logger=self.logger,step_order=step_order,raise_event=self.raise_event)
        elif str(step_name).lower() == "filter":
            return Filter(config=self.config, logger=self.logger,step_order=step_order,raise_event=self.raise_event)
        elif str(step_name).lower() == "enrich":
            return Enrich(config=self.config, logger=self.logger,step_order=step_order,raise_event=self.raise_event)
        elif str(step_name).lower() == "explode":
            return Explode(config=self.config, logger=self.logger,step_order=step_order,raise_event=self.raise_event)
        elif str(step_name).lower() == "add_attributes":
            return AddAttributes(config=self.config, logger=self.logger,step_order=step_order,raise_event=self.raise_event)
        elif str(step_name).lower() == "flow_filter":
            return FlowFilter(config=self.config, logger=self.logger,step_order=step_order,raise_event=self.raise_event)
        elif str(step_name).lower() == "bind":
            return Bind(config=self.config, env_config = self.env_config, logger=self.logger,step_order=step_order,raise_event=self.raise_event)
        elif str(step_name).lower() == "produce":
            return Produce(config=self.config, env_config = self.env_config, logger=self.logger,step_order=step_order,raise_event=self.raise_event)

    def execute_flow(self,data,payload):
        res = True
        self.msg = data
        pos = 0
        for step in self.steps:
            pos += 1
            if step.name == "hl7_parser":
                res = step.pre_execute(self.msg,payload)
                if res:
                    res = step.execute(self.msg,payload)
                    if not res:
                        #self.msg = step.error_attrib 
                        return res
                    self.msg = step.msg
               
            else:
                res = step.execute(self.msg,payload)
                if not res:
                    #self.msg = step.error_attrib 
                    return res
                self.msg = step.msg
        return res
    
    def raise_event(self):
        pass

    def filter_steps(self,field):
        pass



        

    

