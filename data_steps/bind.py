import datetime
import copy
from utils.operator import Operator, OperatorError
from flink_step import FlinkStep, StepError


class BindError(StepError):
    def __init__(self, description):
        super().__init__()
        self.msg = description


class Bind(FlinkStep):
    def __init__(self,config,logger,step_order):
        super().__init__(config,logger,step_order)
        self.bind_instructions = self.step_config["instuctions"]

    def executer(self,message,payload):
        res = True
        try:
            parsed_msg = copy.copy(message)
            parsed_msg[self.field_name] = Operator.calculate_token(self.enrich_instructions,parsed_msg)
            self.current_messages.append(parsed_msg)
        except OperatorError as e:
            res = False
            error_attrib = {}
            error_attrib["msg"] = "EnrichError Error {}".format(str(e))
            error_attrib["error_type"] = "EnrichError"
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

    