import datetime
import copy
from utils.operator import Operator, OperatorError
from data_steps.base_step import BaseStep, StepError
from utils.operator import Operator


class FlowFilterError(StepError):
    def __init__(self, description):
        super().__init__()
        self.msg = description


class FlowFilter(BaseStep):
    def __init__(self,config,logger,step_order,raise_event):
        super().__init__(config,logger,step_order,raise_event)
        self.instructions = self.step_config["instructions"]
        self.raise_event = raise_event
        self.seperator = self.step_config["seperator"]

    def raise_event(self):
        return self.instructions["sub_entity_id_field"]
        
        

    def executer(self,message,payload):
        res = True
        try:
            parsed_msg = copy.copy(message)
            if self.filter(parsed_msg):
                filter_by = self.instructions["sub_entity_id_field"]
                token = []
                token.append(filter_by) # field:0
                token.append("nnull")# rule:1
                token.append(True)# is constant:2
                token.append("")# constant value:3
                Operator.validate_token(token=token,message=parsed_msg)
                parsed_msg[self.config["filters_key_in_message"]]=self._find_key(parsed_msg,filter_by, self.seperator)
                self.current_messages.append(parsed_msg)
            else:
                self.current_messages.append(parsed_msg)
                self.logger.insert_debug_to_log("Filter.executer order:{}".format(self.order),"Filtered out")
        except Exception as e:
            res = False
            error_attrib = {}
            error_attrib["msg"] = "FlowFilterError Error {}".format(str(e))
            error_attrib["error_type"] = "FlowFilterError"
            error_attrib["error_code"] = self.logger.get_error_code(error_attrib["error_type"])
            error_attrib["error_message"] = "error message:{}, error type:{},error code:{},payload:{}".format(str(e),error_attrib["error_type"],error_attrib["error_code"],payload)
            self.msg = error_attrib
        finally:
             return res

    