import datetime
import copy
from utils.operator import Operator, OperatorError
from data_steps.base_step import BaseStep, StepError
from utils.state_manager import StateManager


class BindError(StepError):
    def __init__(self, description):
        super().__init__()
        self.msg = description


class Bind(BaseStep):
    def __init__(self,config,env_config,logger,step_order,raise_event):
        super().__init__(config,logger,step_order,raise_event)
        self.state = StateManager(self.step_config,env_config,logger)

    def executer(self,message,payload):
        res = False
        try:
            parsed_msg = copy.copy(message)
            if self.filter(parsed_msg):
                count_itterations = 0 
                # in case the message is a parent message we will need to loop all existing childs and send them to target topic if needed
                bind_check = self.state.bind_entity(parsed_msg)
                if bind_check:
                    for key in self.state.messages_2_produce:
                        res = True
                        self.logger.logger.debug("Bind.executer, msg:{}".format(self.state.messages_2_produce[key]))
                        self.state.tag_sent_messages(self.state.messages_2_produce[key])
                        self.current_messages.append(self.state.messages_2_produce[key])
                    
            else:
                self.current_messages.append(message)
                self.logger.insert_debug_to_log("Bind.executer order:{}".format(self.order),"Filtered out") 
        except OperatorError as e:
            res = False
            error_attrib = {}
            error_attrib["msg"] = "BindError Error {}".format(str(e))
            error_attrib["error_type"] = "BindError"
            error_attrib["error_code"] = self.logger.get_error_code(error_attrib["error_type"])
            error_attrib["error_message"] = "error message:{}, error type:{},error code:{},payload:{}".format(str(e),error_attrib["error_type"],error_attrib["error_code"],payload)
            self.msg = error_attrib
        except Exception as e:
            res = False
            error_attrib = {}
            error_attrib["msg"] = "EnrichError Error {}".format(str(e))
            error_attrib["error_type"] = "EnrichError"
            error_attrib["error_code"] = self.logger.get_error_code(error_attrib["error_type"])
            error_attrib["error_message"] = "error message:{}, error type:{},error code:{},payload:{}".format(str(e),error_attrib["error_type"],error_attrib["error_code"],payload)
            self.msg = error_attrib
        finally:
             return res

    