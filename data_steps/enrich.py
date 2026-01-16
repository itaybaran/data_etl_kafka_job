import datetime
import copy
from data_steps.base_step import BaseStep, StepError
import json


class EnrichError(StepError):
    def __init__(self, description):
        super().__init__()
        self.msg = description


class Enrich(BaseStep):
    def __init__(self,config,logger,step_order,raise_event):
        super().__init__(config,logger,step_order,raise_event)

    def executer(self,message,payload):
        res = True
        try:
            parsed_msg = copy.copy(message)
            for enrich_step in self.step_config["instuctions"]: 
                parsed_msg[enrich_step["new_field"]]= self.calculate(parsed_msg,enrich_step)
            self.current_messages.append(parsed_msg)
        except EnrichError as e:
            res = False
            error_attrib = {}
            error_attrib["msg"] = "EnrichError Error {}".format(str(e))
            error_attrib["error_type"] = "EnrichError"
            error_attrib["error_code"] = self.logger.get_error_code(error_attrib["error_type"])
            error_attrib["error_message"] = "error message:{}, error type:{},error code:{},payload:{}".format(str(e),error_attrib["error_type"],error_attrib["error_code"],payload)
            self.msg = error_attrib
        except Exception as e:


            error_attrib = {}
            error_attrib["msg"] = "EnrichError Error {}".format(str(e))
            error_attrib["error_type"] = "EnrichError"
            error_attrib["error_code"] = self.logger.get_error_code(error_attrib["error_type"])
            error_attrib["error_message"] = "error message:{}, error type:{},error code:{},payload:{}".format(str(e),error_attrib["error_type"],error_attrib["error_code"],payload)
            self.msg = error_attrib
        finally:
             return res
        
    def calculate(self,msg,enrich_step):
        try:
            method = enrich_step["method"]
            if method == 'concat':
                seperator = self.step_config["seperator"]
                values = self.build_parapm(msg,enrich_step)
                # convert all data to str
                values = list(map(lambda x: str(x), values))
                return seperator.join(values)
            elif method == 'create_sub_entity':
                seperator = self.step_config["seperator"]
                values = self.build_parapm(msg,enrich_step)
                temp = [value for value in values if value not in ("", None)]
                if len(temp) == len(enrich_step["base_fields"]):
                    return enrich_step["sub_type_name"]
                else:
                    if enrich_step["new_field"] in (msg):
                        return msg[enrich_step["new_field"]]
                    else: return ""
            elif method == 'lte':
                return  ""
            elif method == 'lt':
                return  ""
            elif method == 'nnull':
                return  ""
            elif method == 'fex':
                return  ""
            else:
                raise EnrichError("calculator Error Calculator method is not valid")
        except Exception as e:
            raise EnrichError("calculator Error {}".format(str(e)))
        
    def build_parapm(self,msg,enrich_step):
        try:
            res = []
            seperator = self.step_config["seperator"]
            fields = enrich_step["base_fields"]
            for field in fields:
                res.append(self._find_key(dict=msg,key_str=field,seperator=seperator))
            return res
        except Exception as e:
            error_attrib = {}
            error_attrib["msg"] = "EnrichError Error {}".format(str(e))
            error_attrib["error_type"] = "EnrichError"
            error_attrib["error_code"] = self.logger.get_error_code(error_attrib["error_type"])
            error_attrib["error_message"] = "error message:{}, error type:{},error code:{},payload:{}".format(str(e),error_attrib["error_type"],error_attrib["error_code"])
            self.logger.insert_debug_to_log("build_parapm",json.dumps(error_attrib))
        finally:
            return res
        

    def _find_key(self, dict, key_str, seperator):
        keys_arr =str(key_str).split(sep=seperator)
        for key in keys_arr:
            dict = dict[key]
        return dict
    
    def check_if_empty(self,arr):
        temp=[]
        for value in arr:
            if not (value =="" or value is None):
                temp.append(value)
        return temp


    