import datetime
import copy
from utils.operator import Operator, OperatorError
from flink_steps.flink_step import FlinkStep, StepError


class Validator(FlinkStep):
    def __init__(self,config,logger,step_order):
        super().__init__(config,logger,step_order)
        self.validate_instructions = self.step_config["instuctions"]


    def executer(self,message,payload):
        res = True
        try:
            self.msg = message
            for token in self.validate_instructions:
                Operator.validate_token(token,message)
            self.current_messages.append(message)
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
            

   