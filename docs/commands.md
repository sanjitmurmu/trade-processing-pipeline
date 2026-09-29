# Project Commands

## 1. Docker

Docker is used to run the local infrastructure required by the application:  
Kafka, ZooKeeper, PostgreSQL, and Redis.

### Start all infrastructure

```
docker compose up -d
```

Starts all services defined in docker-compose.yml in detached/background mode.  

Used when starting the project for local development.

### Start a specific service
```
docker compose up -d redis
```

Starts only the Redis container.

Useful when the other infrastructure services are already running and Redis has been added or stopped.

### Check running containers
```
docker compose ps
```
Displays the status of the services defined in docker-compose.yml.

Use this to verify that Kafka, ZooKeeper, PostgreSQL, and Redis are running.

### Stop all infrastructure
```
docker compose down
```
Stops and removes the containers created by the current Docker Compose project.

This does not remove the Docker images.

### Check Docker containers directly
```
docker ps
```
Displays currently running Docker containers.

Useful when you want to see containers beyond the services defined in the current Compose project.

### Access a container's command line
```
docker exec -it trade-redis redis-cli
```
Opens the Redis CLI inside the trade-redis container.

The same docker exec pattern can be used to execute commands inside other containers.

### Important port mappings

| Service    | Host Port | Container Port |
| ---------- | --------: | -------------: |
| Kafka      |      9092 |           9092 |
| ZooKeeper  |      2181 |           2181 |
| PostgreSQL |      5432 |           5432 |
| Redis      |      6379 |           6379 |


The format in Docker Compose is:

    HOST_PORT:CONTAINER_PORT

For example:
```
ports:
      - "6379:6379"
```
means:
```
localhost:6379 → Redis container port 6379
```
### One important thing to remember

We used:

```
docker compose up -d
```
rather than manually starting Kafka, PostgreSQL, Redis, etc. individually because Docker Compose manages the infrastructure defined in our docker-compose.yml as one application stack.

And:  
```
docker compose up -d redis
```
was particularly useful when we added Redis later without needing to restart the whole stack.

## 2. Kafka

Kafka is the messaging backbone of the Trade Processing Pipeline.

### Project Kafka details

| Item | Value |
|---|---|
| Kafka container | `trade-kafka` |
| Bootstrap server | `localhost:9092` |
| Topic | `trade-events` |
| Current partitions | `1` |
| Consumer group | `trade-processor-group` |
| Consumer group consumer | `trade-processor` |

---

### List topics

```bash
docker exec trade-kafka kafka-topics --bootstrap-server localhost:9092 --list
```
Lists all Kafka topics available on the broker.

Example:
```
__consumer_offsets
trade-events
```
__consumer_offsets is an internal Kafka topic used to store consumer-group offsets.

trade-events is the application topic used by the Trade Processing Pipeline.

### Consume messages from the beginning

```bash
docker exec trade-kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic trade-events --from-beginning
```

Reads messages from the beginning of the trade-events topic.

Useful for inspecting the events currently stored in Kafka.

### Consume messages and display offsets and keys

```
docker exec trade-kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic trade-events --from-beginning --property print.offset=true --property print.key=true --timeout-ms 5000
```
Reads messages from the beginning and displays:

- Kafka offset
- Kafka message key
- Message value

Example:
```
Offset:5    T2003    {"tradeId":"T2003","symbol":"NVDA",...}
```
The --timeout-ms 5000 option causes the console consumer to stop after approximately 5 seconds without receiving another message.

Useful for inspecting the topic without leaving a consumer process running indefinitely.

### Check consumer group offsets

```
docker exec trade-kafka kafka-consumer-groups --bootstrap-server localhost:9092 --group trade-processor-group --describe
```

Displays information about the consumer group, including:

- Current committed offset
- Log end offset
- Consumer lag
- Assigned partitions

This is useful for understanding whether the consumer is keeping up with Kafka.

### Reset a consumer group's offset — dry run

```
docker exec trade-kafka kafka-consumer-groups --bootstrap-server localhost:9092 --group trade-processor-group --topic trade-events:0 --reset-offsets --to-offset 7
```

<img width="1829" height="122" alt="image" src="https://github.com/user-attachments/assets/f94e87b7-1ff0-4066-ac35-953bf9c736a2" />

Calculates the new offset but does not execute the reset.

The absence of --execute makes this a dry run.

Use this first before performing an offset reset.


### Execute a consumer offset reset
```
docker exec trade-kafka kafka-consumer-groups --bootstrap-server localhost:9092 --group trade-processor-group --topic trade-events:0 --reset-offsets --to-offset 7 --execute
```
<img width="1812" height="110" alt="image" src="https://github.com/user-attachments/assets/4e931eda-2495-47d1-85c8-819677ff2191" />

Moves the consumer group's committed position to offset 7 for partition 0.

This allows records from that position to be consumed again.

**Important:** Stop the consumer application before resetting offsets.

### Offset reset — important concept

Kafka offsets identify the position of records within a partition.

For example:
```
Offset 2 → T1003
Offset 3 → T2001
Offset 4 → T2002
Offset 5 → T2003
Offset 6 → T2004
Offset 7 → next record
```
If the consumer group's committed offset is 7, it will normally continue from offset 7.

If we reset it to 5, the consumer can replay:
```
Offset 5 → T2003
Offset 6 → T2004
```
Resetting an offset does not delete or modify the Kafka records.

It only changes where that consumer group starts consuming.


### Why we reset the offset during development

We used an offset reset to replay T2003 after its original processing failed because of the Redis serialization problem.

We reset:
```
6 → 5
```
which caused:
```
T2003 → processed again
T2004 → processed again
```
T2003 succeeded after the Redis serialization fix.

T2004 then failed because it had already been persisted in PostgreSQL and its trade_id was unique.

We subsequently moved the consumer group to:
```
7
```
to prevent the already-processed T2004 from continuously blocking later records.

### Important Kafka behavior: failed records can block later records

With a single partition:
```
T2004 (offset 6) ❌
T2005 (offset 7)
T2006 (offset 8)
```
If the consumer repeatedly fails on T2004 and keeps retrying offset 6, it cannot simply skip ahead and process T2005.

This is one reason the project will later introduce:
```
Retry → Backoff → Dead Letter Topic (DLQ)
```
A problematic record can eventually be moved out of the main processing path so later records can continue.

### Kafka message retention vs consumer offset

These are two different concepts.

A Kafka record can still physically exist in the topic even after a consumer group has moved past its offset.

For example:
```
Kafka topic:

Offset 5 → T2003
Offset 6 → T2004
Offset 7 → T2005
```
The consumer group's committed offset might be:
```
7
```
This does not mean offsets 5 and 6 have been deleted.

Kafka retains records according to the topic's retention configuration.

The consumer group's offset simply tells Kafka:

*"For this consumer group, processing has progressed to this position."*

### Important warning

Offset-reset commands modify the state of the consumer group.

Always:

- Stop the consumer application.
- Perform a dry run.
- Verify the target offset.
- Execute the reset.
- Restart the consumer.

Never reset offsets casually in a production environment.

## 3. Redis

Redis is used as the cache for reference data in the Trade Processor.

### Project Redis details

| Item | Value |
|---|---|
| Container | `trade-redis` |
| Host | `localhost` |
| Port | `6379` |
| Spring cache name | `referenceData` |
| Example cache key | `referenceData::NVDA` |

---

### Open Redis CLI

```bash
docker exec -it trade-redis redis-cli
```
Opens the Redis command-line interface inside the trade-redis container.

After running this command, the prompt changes to:
```
127.0.0.1:6379>
```

Redis commands can then be executed directly.


### List cache keys
```
KEYS *
```
Lists keys currently stored in Redis.

Example:
```
referenceData::NVDA
```
Useful during local development for inspecting cache contents.

**Note:** KEYS * should generally not be used on a large production Redis instance because it can scan the entire keyspace. It is acceptable for our small local development environment.

### Read a cached value
```
GET "referenceData::NVDA"
```

Returns the value stored for the specified Redis key.

In our project, the value is JSON serialized by:
```
GenericJackson2JsonRedisSerializer
```
Example:
```
{
  "@class": "com.sanjit.common.dto.ReferenceDataResponse",
  "symbol": "NVDA",
  "exchange": "NASDAQ",
  "currency": "USD",
  "sector": "TECH"
}
```

### Check whether a key exists
```
EXISTS "referenceData::NVDA"
```
Returns:
```
1
```
if the key exists, or:
```
0
```
if it does not exist.

Useful for quickly checking whether a reference-data entry is currently cached.

### Check the remaining TTL
```
TTL "referenceData::NVDA"
```
Returns the remaining time-to-live in seconds.

Possible results include:
```
> 0   → key exists and has this many seconds remaining
-1    → key exists but has no expiration
-2    → key does not exist
```
This becomes useful when we configure cache expiration.

### Delete a cache entry
```
DEL "referenceData::NVDA"
```
Deletes the specified cache entry.

This is useful during development when we want to deliberately create a cache MISS and test the complete flow again.

For example:
```
Redis
  ↓
DEL referenceData::NVDA
  ↓
next NVDA trade
  ↓
CACHE MISS
  ↓
Reference Data Service
  ↓
Redis
```

### Exit Redis CLI
```
EXIT
```
or:
```
QUIT
```
Returns to the normal terminal.

### Understanding the Spring cache key

Our application uses:
```
@Cacheable("referenceData")
```
with:
```
getReferenceData(String symbol)
```
For:
```
getReferenceData("NVDA")
```
Spring creates a Redis key equivalent to:
```
referenceData::NVDA
```
The structure is:
```
cache-name::key
```
Therefore:
```
referenceData::NVDA
referenceData::AAPL
referenceData::JPM
```
can represent separate cached reference-data entries.

### Cache HIT vs Cache MISS

The expected flow is:

Cache MISS
```
Trade Processor
      ↓
Redis
      ↓
NVDA not found
      ↓
Reference Data Service
      ↓
Response
      ↓
Redis stores NVDA
```

Cache HIT
```
Trade Processor
      ↓
Redis
      ↓
NVDA found
      ↓
Cached ReferenceDataResponse
```
On a cache HIT, the HTTP call to the Reference Data Service is avoided.


### Inspecting Redis during development

A useful debugging sequence is:
```
docker exec -it trade-redis redis-cli
```
Then:
```
KEYS *
```
Then:
```
GET "referenceData::NVDA"
```
This lets us verify that the application actually populated Redis with the expected reference data.

## 4. Maven

### Build entire project
mvn clean verify

### Run a specific module
...

## 5. Application

### Trade Producer
Port: 8080

### Trade Processor
Port: 8081

### Reference Data Service
Port: 8082


## 6. PostgreSQL / SQL

### Check a trade
...

### Check all trades
...


## 7. Git

### Check status
...

### Add changes
...

### Commit
...

### Push
...
