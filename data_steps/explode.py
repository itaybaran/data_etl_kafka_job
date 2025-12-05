
import copy
from data_steps.flink_step import FlinkStep, StepError

class ExplodeError(StepError):
    def __init__(self, description):
        super().__init__()
        self.msg = description

class Explode(FlinkStep):
    def __init__(self,config,logger,step_order):
        super().__init__(config,logger,step_order)
        self.explode_instructions = self.step_config["instuctions"]
        self.exploded_messages = []

    def executer(self,message,payload):
        res = True
        raw_mesages = []
        try:
            key = message[self.explode_instructions["explode_key"]]
            if not isinstance(key,list):
                key = [key]
            for element in key:
                new_msg = copy.copy(message)
                for new_field in self.explode_instructions["mapping"]:
                    try:
                        new_msg[new_field] = element[self.explode_instructions["mapping"][new_field]]
                    except Exception as e:
                        pass # implement scenario for error in message creation
                del(new_msg[self.explode_instructions["explode_key"]])
                self.current_messages.append(new_msg)
        except ExplodeError as e:
            res = False
            error_attrib = {}
            error_attrib["msg"] = "ExplodeError Error {}".format(str(e))
            error_attrib["error_type"] = "ExplodeError"
            error_attrib["error_code"] = self.logger.get_error_code(error_attrib["error_type"])
            error_attrib["error_message"] = "error message:{}, error type:{},error code:{},payload:{}".format(str(e),error_attrib["error_type"],error_attrib["error_code"],payload)
            self.msg = error_attrib
        except Exception as e:
            res = False
            error_attrib = {}
            error_attrib["msg"] = "ExplodeError Error {}".format(str(e))
            error_attrib["error_type"] = "ExplodeError"
            error_attrib["error_code"] = self.logger.get_error_code(error_attrib["error_type"])
            error_attrib["error_message"] = "error message:{}, error type:{},error code:{},payload:{}".format(str(e),error_attrib["error_type"],error_attrib["error_code"],payload)
            self.msg = error_attrib
        finally:
             return res

    