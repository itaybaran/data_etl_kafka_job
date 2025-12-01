# utils/flink_main.py

import os
import json
import socket
import hashlib
import uuid
from dataclasses import dataclass
from typing import Optional, Dict, Any
from pathlib import Path
from dotenv import load_dotenv, find_dotenv
from configuration import Config
from logger import Logger
from kafka_producer import ConfluentProducer
from kafka_consumer import ConfluentConsumer
import json
import yaml
import os


# ========================
# Runtime / Env bootstrap
# ========================
# Finds the nearest .env walking up from current file/cwd
load_dotenv(find_dotenv())   # or load_dotenv(Path(__file__).with_name(".env"))
# path to Python in your devcontainer venv
PY = os.getenv("PY", "/home/vscode/.venv/bin/python")  # default fallback
APP_ENV   = os.getenv("APP_ENV", "production")  # default fallback
DEBUG     = os.getenv("DEBUG", "false").lower() == "true"
PORT      = int(os.getenv("PORT", "8000"))
SECRET    = os.getenv("SECRET_KEY")             # keep out of source control
STATE_CHECKPOINTS_DIR =  os.getenv("STATE_CHECKPOINTS_DIR")
CONFIG_FILE_PATH = os.getenv("CONFIG_FILE_PATH")
PARALLELISM = int(os.getenv("PARALLELISM"))
OUTPUT_TOPIC   = os.getenv("OUTPUT_TOPIC")  
ERROR_TOPIC   = os.getenv("ERROR_TOPIC") 
AUDIT_TOPIC   = os.getenv("AUDIT_TOPIC")

# Load YAML configuration
main_config = Config(CONFIG_FILE_PATH)
data_config = main_config.get_config()


# --- Add connector JARs (if needed; harmless if already on classpath) ---
input_kafka_properties = data_config["kafka"]["input_kafka_properties"]
input_kafka_properties["client.id"] = socket.gethostname()
output_kafka_properties = data_config["kafka"]["output_kafka_properties"]

# ========================
# Graph wiring
# ========================
def main():
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
    
