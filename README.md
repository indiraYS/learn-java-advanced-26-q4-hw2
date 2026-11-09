# homework-2

## Modules

- **server** — gRPC server exposing `ConversationService`
- **grpcclient1** — gRPC client (Java)
- **grpcclient2** — gRPC client (Kotlin)
- **avro-user-event-exchanger** — Kafka producer + consumer exchanging Avro-serialized `UserEvent` messages

## Prerequisites

### 1. Kafka (required for `avro-user-event-exchanger`)

`AvroUserEventExchanger` connects to Kafka at the address set by the `KAFKA_BOOTSTRAP_SERVERS` environment variable (default: `localhost:9092`). Start the broker using the provided Docker Compose file:

```bash
docker-compose up -d
```

This starts:
- **Kafka** on `localhost:9092` (KRaft mode, no ZooKeeper)
- **Kafka UI** on `http://localhost:8080`

> **Note:** `AvroUserEventExchanger` automatically creates the `user_events` topic on startup if it does not already exist. Do not run it against a production or shared consumers-only cluster, as it will produce messages into that topic.

### 2. gRPC server (required for `grpcclient1` and `grpcclient2`)

Both gRPC clients connect to the server on `localhost`. The port is controlled by the `GRPC_SERVER_PORT` environment variable (default: `8089`). All three modules — `server`, `grpcclient1`, and `grpcclient2` — read this variable, so setting it once is enough.

Start the server module first by running `Main` in the `server` module:

```bash
# Default port 8089
mvn exec:java -Dexec.mainClass="indira.yessimova.secondhomework.Main"

# Custom port
GRPC_SERVER_PORT=9090 mvn exec:java -Dexec.mainClass="indira.yessimova.secondhomework.Main"
```

## Build

```bash
mvn clean package
```

## Running

### `avro-user-event-exchanger`

This module does not produce a fat jar. Run it directly via Maven:

```bash
# Default Kafka at localhost:9092
mvn exec:java -Dexec.mainClass="indira.yessimova.avro.AvroUserEventExchanger" -pl avro-user-event-exchanger

# Custom Kafka address
KAFKA_BOOTSTRAP_SERVERS=localhost:19092 mvn exec:java -Dexec.mainClass="indira.yessimova.avro.AvroUserEventExchanger" -pl avro-user-event-exchanger
```

### `server`, `grpcclient1`, `grpcclient2`

These modules produce a fat jar (`*-jar-with-dependencies.jar`) under `target/`. Run with:

```bash
java -jar server/target/server-1.0-SNAPSHOT-jar-with-dependencies.jar
java -jar grpcclient1/target/grpcclient1-1.0-SNAPSHOT-jar-with-dependencies.jar
java -jar grpcclient2/target/grpcclient2-1.0-SNAPSHOT-jar-with-dependencies.jar
```
