from flink_step import FlinkStep
from data_extractor import DataExtractor 
from hl7_parser import HL7Parser
from validator import Validator
from filter import Filter
from enrich import Enrich
from explode import Explode
from add_attributes import AddAttributes

class FlowManagerError:
    def __init__(self, description):
        super().__init__()
        self.msg = description

class FlowManager:
    def __init__(self,config, logger):
        self.config = config
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
        for step_conf in steps_config:
            if step_conf["is_active"]:
                step = self.create_step(step_conf["name"],step_conf["order"])
                step.name = step_conf["name"]
                step.order = step_conf["order"]
                self.steps.append(step)

    def create_step(self,step_name,step_order):
        if str(step_name).lower() == "hl7_parser":
            return HL7Parser(config=self.config, logger=self.logger,step_order=step_order)
        elif str(step_name).lower() == "data_extractor":
            return DataExtractor(config=self.config, logger=self.logger,step_order=step_order)
        elif str(step_name).lower() == "validator":
            return Validator(config=self.config, logger=self.logger,step_order=step_order)
        elif str(step_name).lower() == "filter":
            return Filter(config=self.config, logger=self.logger,step_order=step_order)
        elif str(step_name).lower() == "enrich":
            return Enrich(config=self.config, logger=self.logger,step_order=step_order)
        elif str(step_name).lower() == "explode":
            return Explode(config=self.config, logger=self.logger,step_order=step_order)
        elif str(step_name).lower() == "add_attributes":
            return AddAttributes(config=self.config, logger=self.logger,step_order=step_order)

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
                        self.msg = step.error_attrib 
                        return res
                    self.msg = step.msg
            else:
                res = step.execute(self.msg,payload)
                if not res:
                    self.msg = step.error_attrib 
                    return res
                self.msg = step.msg
        return res



        

    

