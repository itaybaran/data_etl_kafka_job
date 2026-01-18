
# data_etl_kafka_job

## Overview
This project implements a **Kafka-based ETL / streaming pipeline** written in Python, designed to run inside **VS Code Dev Containers** using **Podman**.
It consumes events from Kafka, processes them through configurable flow steps, optionally enriches/binds state via Redis, and produces results back to Kafka.

The project is designed for **local development on Windows** and **containerized execution** with minimal dependencies on the host.

---

## Architecture
- **Kafka (Confluent, KRaft mode)** – event streaming backbone
- **Redis** – state binding / enrichment
- **Python** – Kafka consumer, flow manager, processing steps
- **VS Code Dev Containers** – isolated developer environment
- **Podman** – container runtime

---

## Project Structure
```
data_etl_kafka_job/
├── configurations/
│   └── configuration.yml
├── data_steps/
│   ├── base_step.py
│   └── *.py
├── utils/
│   ├── kafka_consumer.py
│   ├── state_manager.py
│   └── main.py
├── requirements.txt
├── .env
└── devcontainer.json
```

---

## Runtime Flow
1. Kafka consumer subscribes to configured topic(s)
2. Messages are deserialized and passed to the Flow Manager
3. Flow steps are executed sequentially (as defined in YAML)
4. Redis is used for state binding (parent/child/entity relations)
5. Output events are produced to Kafka

---

## Redis Key Model
Keys are **not** `user:*`.
They follow the pattern:
```
<sub_entity_id>:<parent_key>:<key>
```

Example:
```
12345:patient:visit_date
```

Use:
```redis
SCAN 0 MATCH * COUNT 100
```

---

## How to Run (Developer)
1. Start Redis and Kafka (see DEVOPS_MANUAL.md)
2. Open project in VS Code
3. Reopen in Dev Container
4. Install dependencies (automatic on container start)
5. Run:
```bash
python utils/main.py
```

---

## Common Pitfalls
- Kafka bootstrap server must be `kafka:9092`
- Redis host must be `redis`
- Ensure Redis DB index matches CLI (`db=0`)
- Do not use `localhost` inside containers

---

## Intended Usage
- Local development
- ETL prototyping
- Streaming data enrichment
- Kafka-based pipelines

---

