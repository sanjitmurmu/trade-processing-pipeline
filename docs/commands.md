# Project Commands

## 1. Docker

### Start all infrastructure
docker compose up -d

### Start Redis only
docker compose up -d redis

### Check running containers
docker compose ps

### Stop infrastructure
docker compose down


## 2. Kafka

### List topics
...

### Consume trade-events from beginning
...

### Check consumer group
...

### Reset consumer offset

This command will only show if the offset reset is possible, it won't execute the changes.

    docker exec trade-kafka kafka-consumer-groups --bootstrap-server localhost:9092 --group trade-processor-group --topic trade-events:0 --reset-offsets --to-offset 7

This command will execute offset reset to the new offset position we want

    docker exec trade-kafka kafka-consumer-groups --bootstrap-server localhost:9092 --group trade-processor-group --topic trade-events:0 --reset-offsets --to-offset 7 --execute


## 3. Redis

### Open Redis CLI
...

### List cache keys
...

### Read a cached value
...


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
