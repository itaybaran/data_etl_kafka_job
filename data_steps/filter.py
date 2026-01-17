import datetime
import copy
from utils.operator import Operator, OperatorError
from data_steps.base_step import BaseStep, StepError


class FilterError(StepError):
    def __init__(self, description):
        super().__init__()
        self.msg = description


class Filter(BaseStep):
    def __init__(self,config,logger,step_order,raise_event):
        super().__init__(config,logger,step_order,raise_event)
        self.filter_instructions = self.step_config["instuctions"]

    def executer(self,message,payload):
        res = True
        try:
            parsed_msg = copy.copy(message)
            if self.filter(parsed_msg):
                for token in self.filter_instructions:
                    if Operator.check_operator(token,parsed_msg):
                        self.current_messages.append(parsed_msg)
            else:
                self.current_messages.append(parsed_msg)
                self.logger.insert_debug_to_log("Filter.executer order:{}".format(self.order),"Filtered out")
        except OperatorError as e:
            res = False
            error_attrib = {}
            error_attrib["msg"] = "FilterError Error {}".format(str(e))
            error_attrib["error_type"] = "FilterError"
            error_attrib["error_code"] = self.logger.get_error_code(error_attrib["error_type"])
            error_attrib["error_message"] = "error message:{}, error type:{},error code:{},payload:{}".format(str(e),error_attrib["error_type"],error_attrib["error_code"],payload)
            self.msg = error_attrib
        except Exception as e:
            res = False
            error_attrib = {}
            error_attrib["msg"] = "FilterError Error {}".format(str(e))
            error_attrib["error_type"] = "FilterError"
            error_attrib["error_code"] = self.logger.get_error_code(error_attrib["error_type"])
            error_attrib["error_message"] = "error message:{}, error type:{},error code:{},payload:{}".format(str(e),error_attrib["error_type"],error_attrib["error_code"],payload)
            self.msg = error_attrib
        finally:
             return res

    