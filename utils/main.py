# utils/flink_main.py

import os
from dataclasses import dataclass
from dotenv import load_dotenv, find_dotenv
from configuration import Config
from logger import Logger
from kafka_producer import ConfluentProducer
from kafka_consumer import ConfluentConsumer
import os

def set_from_env():
    env_config = {}
    env_config["PY"] = os.getenv("PY", "/home/vscode/.venv/bin/python")  # default fallback
    env_config["APP_ENV"]   = os.getenv("APP_ENV", "production")  # default fallback
    env_config["DEBUG"]     = os.getenv("DEBUG", "false").lower() == "true"
    env_config["PORT"]      = int(os.getenv("PORT", "8000"))
    env_config["SECRET"]    = os.getenv("SECRET_KEY")             # keep out of source control
    env_config["STATE_CHECKPOINTS_DIR"] =  os.getenv("STATE_CHECKPOINTS_DIR")
    env_config["CONFIG_FILE_PATH"] = os.getenv("CONFIG_FILE_PATH")
    env_config["PARALLELISM"] = int(os.getenv("PARALLELISM"))
    env_config["OUTPUT_TOPIC"]   = os.getenv("OUTPUT_TOPIC")  
    env_config["ERROR_TOPIC"]   = os.getenv("ERROR_TOPIC") 
    env_config["AUDIT_TOPIC"]   = os.getenv("AUDIT_TOPIC")
    # Load YAML configuration
    main_config = Config(env_config["CONFIG_FILE_PATH"])
    data_config = main_config.get_config()
    dict={}
    dict["main_config"] = main_config
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
    load_dotenv(find_dotenv())   # or load_dotenv(Path(__file__).with_name(".env"))
    # path to Python in your devcontainer venv
    data_config = set_from_env()
    # --- Set kafka properties
    #kafka_properties = data_config["kafka"]["kafka_properties"]
    #kafka_properties["client.id"] = socket.gethostname()
    # Build per-topic sources → tagged union
    try:
        topics = []
        logger = Logger()
        logger.insert_debug_to_log("main","enter function")
        for part in data_config["kafka"]["parts"]:
            topics.append(part["topic"])
            bootstrap = part["bootstrap"]
            name = part["name"]
        data_config["kafka"]["INPUT_TOPICS"] = topics
        consumer = ConfluentConsumer(data_config,logger)
        consumer.consume()
        
        logger.insert_debug_to_log("main","execute data_etl_kafka_job")
        logger.insert_debug_to_log("main","end function")
    except Exception as e:
        logger.insert_error_to_log(-101,"Flink Environment issue:{}".format(str(e)))


# Execute
if __name__ == "__main__":
    main()
    
