import datetime
import copy
from utils.operator import Operator, OperatorError
from data_steps.base_step import BaseStep, StepError

class ValidatorError(StepError):
    def __init__(self, description):
        super().__init__()
        self.msg = description

class Validator(BaseStep):
    def __init__(self,config,logger,step_order,raise_event):
        super().__init__(config,logger,step_order,raise_event)
        self.validate_instructions = self.step_config["instuctions"]


    def executer(self,message,payload):
        res = True
        try:
            parsed_msg = copy.copy(parsed_msg)
            if self.filter(parsed_msg):
                self.msg = parsed_msg
                for token in self.validate_instructions:
                    Operator.validate_token(token,parsed_msg)
                self.current_messages.append(parsed_msg)
            else:
                self.current_messages.append(parsed_msg)
                self.logger.insert_debug_to_log("Validator.executer order:{}".format(self.order),"Filtered out")
        except AssertionError as e:
            res = False
            
            error_attrib = {}
            error_attrib["msg"] = "ValidatorError Error {}".format(str(e))
            error_attrib["error_type"] = "ValidatorError"
            error_attrib["error_code"] = self.logger.get_error_code(error_attrib["error_type"])
            error_attrib["error_message"] = "error message:{}, error type:{},error code:{},payload:{}".format(str(e),error_attrib["error_type"],error_attrib["error_code"],payload)
            self.msg = error_attrib
        except OperatorError as e:
            res = False
            error_attrib = {}
            error_attrib["msg"] = "OperatorError Error {}".format(str(e))
            error_attrib["error_type"] = "OperatorError"
            error_attrib["error_code"] = self.logger.get_error_code(error_attrib["error_type"])
            error_attrib["error_message"] = "error message:{}, error type:{},error code:{},payload:{}".format(str(e),error_attrib["error_type"],error_attrib["error_code"],payload)
            self.msg = error_attrib
        except Exception as e:
            res = False
            error_attrib = {}
            error_attrib["msg"] = "ValidatorError Error {}".format(str(e))
            error_attrib["error_type"] = "ValidatorError"
            error_attrib["error_code"] = self.logger.get_error_code( error_attrib["error_type"])
            error_attrib["error_message"] = "error message:{}, error type:{},error code:{},payload:{}".format(str(e),error_attrib["error_type"],error_attrib["error_code"],payload)
            self.msg = error_attrib
        finally:
            return res
            

   