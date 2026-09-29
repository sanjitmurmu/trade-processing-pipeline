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

<img width="1829" height="122" alt="image" src="https://github.com/user-attachments/assets/f94e87b7-1ff0-4066-ac35-953bf9c736a2" />


This command will execute offset reset to the new offset position we want

    docker exec trade-kafka kafka-consumer-groups --bootstrap-server localhost:9092 --group trade-processor-group --topic trade-events:0 --reset-offsets --to-offset 7 --execute

<img width="1812" height="110" alt="image" src="https://github.com/user-attachments/assets/4e931eda-2495-47d1-85c8-819677ff2191" />



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
