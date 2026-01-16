# utils/main.py
from dataclasses import dataclass
from dotenv import dotenv_values
from utils.configuration import Config
from utils.logger import Logger
from utils.kafka_producer import ConfluentProducer
from utils.kafka_consumer import ConfluentConsumer

def set_from_env():
    env_config = {}
    env_vars = dotenv_values(".env")  # returns a dict
    for key, value in env_vars.items():
        env_config[key] = value
    env_config["INPUT_TOPICS"] =  env_config["INPUT_TOPICS"].split(",")
    # Load YAML configuration
    dict={}
    main_config = Config(env_config["CONFIG_FILE_PATH"])
    dict["main_config"] = main_config.get_config()
    dict["env_config"] = env_config
    return dict

# ========================
# Graph wiring
# ========================
def main():
        # ========================
    # Runtime / Env bootstrap
    # ========================
    # Finds the nearest .env walking up from current file/cwd
    # path to Python in your devcontainer venv
    data_config = set_from_env()
    # --- Set kafka properties
    #kafka_properties = data_config["kafka"]["kafka_properties"]
    #kafka_properties["client.id"] = socket.gethostname()
    # Build per-topic sources → tagged union
    try:
        logger = Logger()
        logger.insert_debug_to_log("main","enter function")
        consumer = ConfluentConsumer(data_config,logger)
        consumer.consume()
        
        logger.insert_debug_to_log("main","execute data_etl_kafka_job")
        logger.insert_debug_to_log("main","end function")
    except Exception as e:
        logger.insert_error_to_log(-101,"Environment issue:{}".format(str(e)))
        

# Execute
if __name__ == "__main__":
    main()
    
