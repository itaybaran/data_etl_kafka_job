import os
import copy
from data_steps.flink_step import FlinkStep, StepError
from utils.state_manager import StateManager
from utils.kafka_producer import ConfluentProducer


class ProduceError(StepError):
    def __init__(self, description):
        super().__init__()
        self.msg = description


class Produce(FlinkStep):
    def __init__(self,config,env_config,logger,step_order):
        super().__init__(config,logger,step_order)
        self.state = StateManager(self.step_config,env_config,logger)
        self.kafka_client_auth = {"bootstrap.servers": self.step_config["instructions"]["bootstrap.servers"],
                                  "security.protocol": self.step_config["instructions"]["security.protocol"]}
        self.poducer = ConfluentProducer(self.step_config["instructions"]["topic"],self.step_config["instructions"]["batch_size"],self.kafka_client_auth)

    def executer(self,message,payload):
        res = True
        try:
            self.poducer.send(message)
            self.current_messages.append(self.state.current_message)
        except ProduceError as e:
            res = False
            error_attrib = {}
            error_attrib["msg"] = "ProduceError Error {}".format(str(e))
            error_attrib["error_type"] = "ProduceError"
            error_attrib["error_code"] = self.logger.get_error_code(error_attrib["error_type"])
            error_attrib["error_message"] = "error message:{}, error type:{},error code:{},payload:{}".format(str(e),error_attrib["error_type"],error_attrib["error_code"],payload)
            self.msg = error_attrib
        except Exception as e:
            res = False
            error_attrib = {}
            error_attrib["msg"] = "ProduceError Error {}".format(str(e))
            error_attrib["error_type"] = "ProduceError"
            error_attrib["error_code"] = self.logger.get_error_code(error_attrib["error_type"])
            error_attrib["error_message"] = "error message:{}, error type:{},error code:{},payload:{}".format(str(e),error_attrib["error_type"],error_attrib["error_code"],payload)
            self.msg = error_attrib
        finally:
             return res

    