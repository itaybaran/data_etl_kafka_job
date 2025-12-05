import datetime
import xml.etree.ElementTree as ET
import copy
from flink_steps.flink_step import FlinkStep, StepError


class HL7ParserError(StepError):
    def __init__(self, description):
        super().__init__()
        self.msg = description


class Hl7_input(FlinkStep):
    def __init__(self, config,logger,step_order):
        super().__init__(config,logger,step_order)
        self.config = config
        self.hl7_src = None
        self.spliters = self.step_config["hl7_spliters"]
        self.hl7_src_arr = None
        self.logger = logger
        self.hl7_data_model_code = self.step_config["hl7_data_model_code"]
        self.attributes = self.step_config["attributes"]
        self.hl7_key = self.step_config["hl7_key"]

    def split_rows(self,src_arr, spliters, stop=False):
        try:
            if not stop:
                stop = True
                start_length = len(src_arr)
                for spliter in spliters:
                    for row in src_arr:
                        arr = str(row).split(spliter)
                        if len(arr)>1 and len(spliters)>0:
                            arr[1]= "{}{}".format(spliter, arr[1])
                            src_arr.remove(row)
                            src_arr.append(arr[0])
                            src_arr.append(arr[1])
                            stop = False
                            spliters.remove(spliter)
                            break
                    if len(src_arr) > start_length:
                        self.split_rows(src_arr,spliters,stop)
            return src_arr
        except Exception as e:
            res = False
            error_attrib = {}
            error_attrib["msg"] = "HL7ParserError Error {}".format(str(e))
            error_attrib["error_type"] = "HL7ParserError"
            error_attrib["error_code"] = self.logger.get_error_code(error_attrib["error_type"])
            error_attrib["error_message"] = "error message:{}, error type:{},error code:{},payload:{}".format(str(e),error_attrib["error_type"],error_attrib["error_code"],payload)
            self.msg = error_attrib
                    
    
class HL7Parser(FlinkStep):
    def __init__(self,config,logger,step_order):
        super().__init__(config,logger,step_order)
        self.Hl7_input = Hl7_input(config,logger,step_order)

    def pre_execute(self,message,payload):
        try:
            res = True
            xml_tag = self.step_config["xml_tag"]
            key = self.step_config["hl7_json_key"]
            key_arr = key.split(".")
            xml_data = message
            for key in key_arr:
                xml_data= xml_data[key]
            self.Hl7_input.hl7_src = xml_data.split("<{}>".format(xml_tag))[1].split("</{}>".format(xml_tag))[0]
            message = self.Hl7_input.hl7_src
        except Exception as e:
            res = False
            error_attrib = {}
            error_attrib["msg"] = "HL7ParserError Error {}".format(str(e))
            error_attrib["error_type"] = "HL7ParserError"
            error_attrib["error_code"] = self.logger.get_error_code( error_attrib["error_type"])
            error_attrib["error_message"] = "error message:{}, error type:{},error code:{},payload:{}".format(str(e),error_attrib["error_type"],error_attrib["error_code"],payload)
            self.msg = error_attrib
        finally: 
            return res


    def executer(self,msg,payload):
        try:
            res = True
            self.response = {
            "Hl7_input": {"hl7_src": self.Hl7_input.hl7_src, "hl7_data_model_code": self.Hl7_input.hl7_data_model_code,
                          "attributes": self.Hl7_input.attributes}, "is_success": True, "error_code": None,
            "error_msg": "", "hl7_key": self.Hl7_input.hl7_key}
            data = {}
            spliters = copy.copy(self.Hl7_input.spliters)
            hl7_rows = self.Hl7_input.split_rows([self.Hl7_input.hl7_src],spliters,False)[1:]
            for row in hl7_rows:
                pipe_pos = 0
                pipes = str(row).split("|")
                pipe_id = pipes[0]
                data[pipe_id] = {}
                for pipe in pipes:
                    hut_pos = 1
                    for hut in str(pipe).split("^"):
                        key = "{}_{}_{}".format(pipe_id, pipe_pos, hut_pos)
                        data[pipe_id][key] = hut
                        hut_pos += 1
                    pipe_pos += 1
            self.msg = data
            msg = self.msg
        except Exception as e:
            res = False
            error_attrib = {}
            error_attrib["msg"] = "HL7ParserError Error {}".format(str(e))
            error_attrib["error_type"] = "HL7ParserError"
            error_attrib["error_code"] = self.logger.get_error_code( error_attrib["error_type"])
            error_attrib["error_message"] = "error message:{}, error type:{},error code:{},payload:{}".format(str(e),error_attrib["error_type"],error_attrib["error_code"],payload)
            self.msg = StepError(error_attrib)
        finally:
            return res
            