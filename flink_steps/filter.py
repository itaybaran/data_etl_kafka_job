import datetime
import copy
from utils.operator import Operator, OperatorError
from flink_steps.flink_step import FlinkStep, StepError


class FilterError(StepError):
    def __init__(self, description):
        super().__init__()
        self.msg = description


class Filter(FlinkStep):
    def __init__(self,config,logger,step_order):
        super().__init__(config,logger,step_order)
        self.filter_instructions = self.step_config["instuctions"]

    def executer(self,message,payload):
        res = True
        try:
            for token in self.filter_instructions:
                if Operator.check_operator(token,message):
                    self.current_messages.append(message)
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

    