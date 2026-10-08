# Trade Processing Pipeline

A backend microservice application built with Java, Spring Boot, Apache Kafka, PostgreSQL, and Redis for processing financial trade events asynchronously.

The project is designed as a hands-on backend engineering project focused on event-driven architecture, distributed systems, reliability, fault tolerance, data consistency, and production-oriented backend design.

---

## Architecture

                         ┌─────────────────────┐
                         │       Client        │
                         │   Postman / REST    │
                         └──────────┬──────────┘
                                    │
                                    │ POST /api/trades
                                    ▼
                         ┌─────────────────────┐
                         │   Trade Producer    │
                         │    Spring Boot      │
                         └──────────┬──────────┘
                                    │
                                    │ TradeEvent
                                    ▼
                         ┌─────────────────────┐
                         │       Kafka         │
                         │    trade-events     │
                         └──────────┬──────────┘
                                    │
                                    │ Consume
                                    ▼
                         ┌─────────────────────┐
                         │   Trade Processor   │
                         │    Spring Boot      │
                         └──────────┬──────────┘
                                    │
                       ┌────────────┴────────────┐
                       │                         │
                       ▼                         ▼
              ┌─────────────────┐      ┌─────────────────┐
              │   PostgreSQL    │      │      Redis      │
              │                 │      │ Reference Data  │
              │ trade           │      │     Cache       │
              │ outbox_event    │      └─────────────────┘
              └────────┬────────┘
                       │
                       │ PENDING Outbox Events
                       ▼
              ┌─────────────────────┐
              │  Outbox Publisher   │
              │  Scheduled Worker   │
              └──────────┬──────────┘
                         │
                         │ Publish
                         ▼
              ┌─────────────────────┐
              │       Kafka         │
              │ trade-notifications │
              └─────────────────────┘
                         │
                         ▼
              ┌─────────────────────┐
              │ Notification        │
              │ Service             │
              │    (Scaffolded)     │
              └─────────────────────┘

The architecture is implemented as a Maven multi-module project. Each service is a separate Spring Boot application and can be built and deployed independently.

## Modules

| Module                   | Responsibility                                          | Status      |
| ------------------------ | ------------------------------------------------------- | ----------- |
| `trade-common`           | Shared DTOs, enums, constants and common components     | In progress |
| `trade-producer`         | Exposes REST API and publishes trade events to Kafka    | Implemented |
| `trade-processor`        | Consumes Kafka events and persists trades to PostgreSQL | Implemented |
| `reference-data-service` | Provides reference data and enrichment capabilities     | Scaffolded  |
| `notification-service`   | Handles downstream trade notifications                  | Scaffolded  |


## Technology Stack

- Java 20
- Spring Boot 3.5
- Spring Kafka
- Apache Kafka
- PostgreSQL 16
- Redis
- Spring Data JPA
- Spring Cache
- Maven
- Docker / Docker Compose
- Lombok

## Current Processing Flow

The current end-to-end flow is:
```    
              REST Request
                   ↓
              Trade Producer
                   ↓
              TradeEvent
                   ↓
              Kafka: trade-events
                   ↓
              Trade Processor
                   ↓
              Reference Data Enrichment
                   ↓
              Duplicate Check
                   ↓
              ┌─────────────────────────────────────┐
              │ PostgreSQL Transaction              │
              │                                     │
              │  TradeEntity                        │
              │       +                             │
              │  OutboxEvent (PENDING)              │
              └─────────────────────────────────────┘
                   ↓
              Outbox Publisher
                   ↓
              Kafka: trade-notifications
```
## Trade Processing
1. Trade submission

A client sends a trade through the REST API:

    POST /api/trades

Example request:

    {
      "tradeId": "T1001",
      "symbol": "AAPL",
      "side": "BUY",
      "quantity": 100,
      "price": 190.50
    }

2. Kafka publishing

trade-producer receives the request and publishes the TradeEvent to the Kafka topic:

    trade-events

Spring Kafka's JsonSerializer converts the Java event into bytes that Kafka can transport and store.

3. Kafka consumption

trade-processor consumes messages from:
```
Topic:
trade-events

Consumer Group:
trade-processor-group
```
The processor deserializes the Kafka message back into a TradeEvent.

4. Reference-data enrichment

The processor obtains reference information required for trade processing, such as:
- Exchange
- Currency
- Sector
- Other reference attributes
Redis is used as a cache for reference data to avoid unnecessary repeated lookups.  
Example cache:

```
Cache:
referenceData

Example key:
referenceData::NVDA
```

5. Idempotent processing

Before persisting a trade, the processor checks whether the tradeId already exists.  
If the trade has already been processed, the duplicate event is skipped.  
This protects the database from creating duplicate trade records when the same Kafka event is delivered more than once.  

## Transactional Outbox Pattern
The trade processor uses the Transactional Outbox Pattern to maintain consistency between database persistence and downstream event publishing.  
When a trade is successfully processed, the following operations occur inside the same PostgreSQL transaction:  
```
┌──────────────────────────────────┐
│ PostgreSQL Transaction           │
│                                  │
│  Save TradeEntity                │
│           +                      │
│  Save OutboxEvent (PENDING)      │
│                                  │
│  COMMIT                          │
└──────────────────────────────────┘
```

This ensures that the trade and its corresponding outbox event are committed atomically.
If the transaction fails, both operations are rolled back.


**Outbox Event**

The outbox table stores events that must eventually be published to Kafka.
Important fields include:  
```
id
event_type
aggregate_id
topic
payload
status
created_at
published_at
```

The initial status is:

    PENDING

## Outbox Publisher
A scheduled publisher periodically checks for pending outbox events.

```
PostgreSQL
    │
    │ PENDING events
    ▼
Outbox Publisher
    │
    │ Kafka send
    ▼
trade-notifications
```
When Kafka successfully acknowledges the send:  

      PENDING → PUBLISHED
      
and published_at is recorded.  

If Kafka is unavailable, the event remains:

    PENDING
and the next scheduled execution can retry publishing it.  

The current implementation runs the publisher once every 60 seconds.

## Delivery Semantics

The current outbox implementation provides at-least-once delivery semantics.  

A possible failure window exists if:
```
Kafka publish succeeds
        ↓
Application crashes
        ↓
Outbox status is still PENDING
```

The event may therefore be published again during a later retry.  

The system is designed to tolerate this through idempotent processing rather than risk losing an event.

## Kafka Error Handling

The trade consumer uses Spring Kafka retry support.  

Current configuration:
```
Attempts: 4

Backoff:
Initial delay: 1 second
Multiplier: 2
Maximum delay: 4 seconds
```
If processing continues to fail after the configured retry attempts, the message is handled by the Dead Letter Topic (DLT) mechanism.  

High-level flow:
```
Kafka Message
     ↓
Consumer
     ↓
Processing Failure
     ↓
Retry
     ↓
Retry
     ↓
Retry
     ↓
Retry
     ↓
DLT
```
This prevents a permanently failing message from continuously blocking normal processing.

## Reliability Scenarios Verified

The following scenarios have been tested:  

**Normal processing**
```
Trade Event
    ↓
Kafka
    ↓
Trade Processor
    ↓
Trade + Outbox saved
    ↓
Outbox published
    ↓
PUBLISHED
```

**Kafka unavailable**

```
Trade + Outbox saved
        ↓
Kafka unavailable
        ↓
Publish fails
        ↓
Outbox remains PENDING
        ↓
Kafka recovers
        ↓
Next publisher run succeeds
        ↓
PUBLISHED
```

**Transaction atomicity**

If the transaction fails:

```
Trade save
    +
Outbox save
    ↓
Transaction failure
    ↓
ROLLBACK
    ↓
Neither record remains in PostgreSQL
```

**Duplicate delivery**

If the same trade event is received more than once:
```
First delivery
    ↓
Trade persisted

Second delivery
    ↓
tradeId already exists
    ↓
Duplicate skipped
```
Only one trade and one corresponding outbox event are persisted.

## Kafka

Kafka acts as the asynchronous event backbone of the application.

**Topics**
```
trade-events
trade-notifications
```

**Consumer Group**
```
trade-processor-group
```

The producer and processor are decoupled through Kafka:
```
Producer → Kafka → Processor
```
This allows the producer to publish events without requiring the processor to be available at the exact same moment.

## Database

PostgreSQL is used for transactional persistence.

**Main tables**
```
trade
outbox_event
```

trade
Stores the processed trade information.  


outbox_event
Stores events that must be published to downstream Kafka topics.  

Outbox event lifecycle:
```
PENDING
   ↓
Kafka publish successful
   ↓
PUBLISHED
```

## Redis

Redis is used as a cache for reference data.  

Example:
```
Cache:
referenceData

Key:
referenceData::NVDA
```
The cache reduces repeated reference-data lookups during trade processing.

## Microservice Structure

Although all services are maintained in a single Git repository, they are separate applications.

    trade-processing-pipeline/
    │
    ├── trade-common/
    │
    ├── trade-producer/
    │
    ├── trade-processor/
    │
    ├── reference-data-service/
    │
    ├── notification-service/
    │
    ├── docs/
    │
    ├── docker-compose.yml
    └── pom.xml

The repository follows a monorepo / multi-module Maven structure while maintaining separate runtime boundaries for the services.

## Service Ports

| Service | Port |
|---|---:|
| `trade-producer` | 8080 |
| `trade-processor` | 8081 |
| `reference-data-service` | 8082 |

**Infrastructure:**

| Component | Port |
|---|---:|
| Kafka | 9092 |
| ZooKeeper | 2181 |
| PostgreSQL | 5432 |
| Redis | 6379 |

## Documentation

Detailed architecture and design documentation is maintained under:

    docs/

Current documentation includes:

- High-Level Design
- Low-Level Design
- API Contracts
- Database Design
- Kafka Topics
- Sequence Diagrams
- Requirements
- Interview Questions
- Development and Docker Commands

## Project Roadmap

The project will evolve incrementally toward a production-style event-driven backend.

**Completed**

- [x] Trade submission REST API
- [x] Kafka producer
- [x] Kafka consumer
- [x] Reference-data enrichment
- [x] Redis caching
- [x] Duplicate / idempotent processing
- [x] Kafka retry handling
- [x] Dead Letter Topic handling
- [x] Transaction management
- [x] Transactional Outbox Pattern
- [x] Outbox retry on Kafka failure
- [x] Trade persistence
- [x] Basic reliability testing

**Planned**
- [ ] Trade status lifecycle
- [ ] Notification processing
- [ ] Automated unit and integration testing
- [ ] Observability and metrics
- [ ] Improved outbox processing and concurrency
- [ ] Containerized service deployment
- [ ] CI/CD pipeline
- [ ] Scalable service deployment
- [ ] Further resilience and failure testing
  
## Learning Objectives

This project is being developed as a practical backend engineering exercise with emphasis on:

- Spring Boot
- Microservice architecture
- Kafka fundamentals and internals
- Event-driven architecture
- Serialization and deserialization
- Kafka producers and consumers
- Consumer groups and offsets
- Database persistence
- Transaction management
- Transactional Outbox Pattern
- Idempotency
- Retry and Dead Letter Topics
- Redis caching
- Distributed-system design
- Reliability and fault tolerance
- At-least-once delivery
- Production-oriented backend engineering

