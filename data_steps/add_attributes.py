import datetime
import uuid
import copy
from utils.operator import Operator, OperatorError
from data_steps.flink_step import FlinkStep, StepError


class AddAttributes(FlinkStep):
    def __init__(self,config,logger,step_order):
        super().__init__(config,logger,step_order)
        self.add_attributes_instructions = self.step_config["instuctions"]

    def executer(self,message,payload):
        res = True
        try:
            active_msg = copy.copy(message)
            for attribute in self.add_attributes_instructions:
                if attribute == "DateTimeKafkaData" and self.add_attributes_instructions[attribute]:
                    active_msg["DateTimeKafkaData"] = str(datetime.datetime.now())
                elif attribute == "MessageIdKafkaData" and self.add_attributes_instructions[attribute]:
                    active_msg["MessageIdKafkaData"] = str(uuid.uuid4())
                elif attribute == "RowNum" and self.add_attributes_instructions[attribute]:
                    active_msg["RowNum"] = len(self.current_messages) + 1
                elif attribute == "MaxRowNum" and self.add_attributes_instructions[attribute]:
                    active_msg["MaxRowNum"] = len(self.msg)
            self.current_messages.append(active_msg)           

        except Exception as e:
            res = False
            error_attrib = {}
            error_attrib["msg"] = "AddAttributes Error {}".format(str(e))
            error_attrib["error_type"] = "AddAttributesError"
            error_attrib["error_code"] = self.logger.get_error_code(error_attrib["error_type"])
            error_attrib["error_message"] = "error message:{}, error type:{},error code:{},payload:{}".format(str(e),error_attrib["error_type"],error_attrib["error_code"],payload)
            self.msg = error_attrib
        finally:
             return res

    