
import copy
from data_steps.base_step import BaseStep, StepError

class ExplodeError(StepError):
    def __init__(self, description):
        super().__init__()
        self.msg = description

class Explode(BaseStep):
    def __init__(self,config,logger,step_order,raise_event):
        super().__init__(config,logger,step_order,raise_event)
        self.explode_instructions = self.step_config["instuctions"]
        self.exploded_messages = []

    def executer(self,message,payload):
        res = True
        raw_mesages = []
        try:
            parsed_msg = copy.copy(message)
            if self.filter(parsed_msg):
                key = message[self.explode_instructions["explode_key"]]
                if not isinstance(key,list):
                    key = [key]
                for element in key:
                    new_msg = copy.copy(parsed_msg)
                    for new_field in self.explode_instructions["mapping"]:
                        try:
                            new_msg[new_field] = element[self.explode_instructions["mapping"][new_field]]
                        except Exception as e:
                            pass # implement scenario for error in message creation
                    del(new_msg[self.explode_instructions["explode_key"]])
                    self.current_messages.append(new_msg)
            else:
                self.current_messages.append(parsed_msg)
                self.logger.insert_debug_to_log("Explode.executer order:{}".format(self.order),"Filtered out") 

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

    