
# DEVOPS MANUAL – data_etl_kafka_job

## Target Environment
- OS: Windows 10/11
- Container runtime: Podman
- Kafka: Confluent Kafka (KRaft mode)
- Network: `devnet`
- Redis: redis:7-alpine
- Dev environment: VS Code Dev Containers

---

## Network
Create once:
```powershell
podman network create devnet
```

---

## Redis Setup
```powershell
podman rm -f redis

$redisData = "$HOME\podman\redis-data"
mkdir $redisData -Force

podman run -d --name redis --restart unless-stopped `
  --network devnet `
  -p 6379:6379 `
  -v ${redisData}:/data `
  redis:7-alpine `
  redis-server --appendonly yes --requirepass "ChangeMe123!"
```

Verify:
```powershell
podman exec -it redis redis-cli -a "ChangeMe123!" PING
```

---

## Kafka (Confluent KRaft – Single Node)

```powershell
podman rm -f kafka

$kafkaData = "$HOME\podman\kafka-kraft-data"
mkdir $kafkaData -Force

podman run -d --name kafka --restart unless-stopped `
  --network devnet `
  -p 9092:9092 `
  -v ${kafkaData}:/var/lib/kafka/data `
  -e KAFKA_NODE_ID=1 `
  -e KAFKA_PROCESS_ROLES="broker,controller" `
  -e KAFKA_CONTROLLER_QUORUM_VOTERS="1@kafka:9093" `
  -e KAFKA_LISTENERS="PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093" `
  -e KAFKA_ADVERTISED_LISTENERS="PLAINTEXT://kafka:9092" `
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP="PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT" `
  -e KAFKA_CONTROLLER_LISTENER_NAMES="CONTROLLER" `
  -e KAFKA_INTER_BROKER_LISTENER_NAME="PLAINTEXT" `
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 `
  -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 `
  -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 `
  -e CLUSTER_ID="MkU3OEVBNTcwNTJENDM2Qk" `
  docker.io/confluentinc/cp-kafka:7.6.0
```

Verify:
```powershell
podman exec -it kafka kafka-topics --bootstrap-server kafka:9092 --list
```

---

## Required Configuration Values

### Kafka
- Bootstrap servers: `kafka:9092`
- Auto topic creation: enabled (dev)

### Redis
- Host: `redis`
- Port: `6379`
- Password: `ChangeMe123!`
- DB index: `0`

---

## Dev Container
`devcontainer.json` must include:
```json
"runArgs": ["--network", "devnet"]
```

---

## Operational Notes
- Do NOT reuse Kafka data directory with different cluster IDs
- Do NOT use localhost inside containers
- Prefer DNS names (`kafka`, `redis`)
- Use `SCAN`, not `KEYS`, in production-scale Redis

---

## Troubleshooting
| Symptom | Cause |
|-------|------|
| Kafka timeout | Wrong advertised listener |
| Redis empty | Wrong DB index |
| Python works, CLI empty | DB mismatch |
| Connection refused | Wrong network |

---
