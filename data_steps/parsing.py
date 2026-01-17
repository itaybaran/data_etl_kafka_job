import decimal
import datetime
import time
import copy
from data_steps.base_step import BaseStep, StepError


class Parsing(BaseStep):
    def __init__(self, config, logger,step_order,raise_event):
        super().__init__(config,logger,step_order,raise_event)
        self._data_description = None
        self._cols_pos = {}
        self.values_dict = {}
        self.parsed_messages = {}

    def executer(self,msg,payload):
        try:
            current_message = copy.copy(msg)
            if self.filter(current_message):
                res = True
                self._load_values(current_message)
                parsed_msg = copy.copy(self.step_config['data_message'])
                for key in self.values_dict:
                    try:
                        parsed_msg = self._set_key_in_dict(parsed_msg, key, key, self.values_dict[key], False)
                    except Exception as e:
                        pass #collect the error as a metric
                self.current_messages.append(parsed_msg)
            else:
                self.current_messages.append(current_message)
                self.logger.insert_debug_to_log("Parse.executer order:{}".format(self.order),"Filtered out")    
        except Exception as e:
            res = False
            error_attrib = {}
            error_attrib["msg"] = "parsing Error {}".format(str(e))
            error_attrib["error_type"] = "parsing"
            error_attrib["error_code"] = self.logger.get_error_code(error_attrib["error_type"])
            error_attrib["error_message"] = "error message:{}, error type:{},error code:{},payload:{}".format(str(e),error_attrib["error_type"],error_attrib["error_code"],payload)
            self.msg = error_attrib
        finally:
            return res 


    def _load_values(self, msg):
        keys_dict = self.step_config['extracted_fields']
        for key in keys_dict:
            value = keys_dict[key]
            try:
                current_value = self._find_key(dict=msg,key_str=value,seperator=self.step_config['seperator'])
            except Exception as e:
                pass #collect the error as a metric
                current_value = ""
            finally:
                self.values_dict[key] = current_value

    @classmethod
    def _set_data_description(self, data_description):
        self._data_description = data_description
        self._set_cols_pos()

    @classmethod
    def _dict_value_from_key(self, dict, extract_key):
        res = self._find_key(dict, extract_key, {})
        return res

    @classmethod
    def _dict_position_from_key(self, dict, extract_key):
        for key in dict:
            if key == extract_key:
                return dict[key]

    def _dict_key_list(self, dict):
        res = ""
        for key in dict:
            res = res + "," + key
        if len(res) > 1:
            return res[1:]
        
    @classmethod
    def _dict_value_list(self, dict):
        res = ""
        for key in dict:
            res = res + "," + dict[key]
        if len(res) > 1:
            return res[1:]
    
    def _set_key_in_dict(self, current_dict, extract_key, extract_key_for_list, new_value, to_stop):
        if not to_stop:
            for key in current_dict:
                if isinstance(current_dict[key], dict):
                    self._set_key_in_dict(current_dict[key], extract_key, extract_key_for_list,new_value, to_stop)
                if extract_key!= extract_key_for_list:
                    extract_key = extract_key_for_list

                if key == extract_key:
                    current_dict[key] = Parsing.json_encode_decimal(new_value)
                    self._set_key_in_dict(current_dict, extract_key, extract_key_for_list,new_value, True)
        return current_dict

    @classmethod
    def _set_cols_pos(self):
        i = 0
        for col in self._data_description:
            key = self._data_description[i][0]
            self._cols_pos[key] = i
            i = i + 1

    @staticmethod
    def json_encode_decimal(obj):
        if isinstance(obj, decimal.Decimal):
            return float(obj)
        elif isinstance(obj,(datetime.date , datetime.datetime)):
            return obj.strftime("%Y-%m-%dT%H:%M:%S")
        else:
            return obj
