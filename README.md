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

## Kafka

Kafka acts as the asynchronous event backbone of the application.

The project currently uses:

    Topic:
    trade-events
    
    Consumer Group:
    trade-processor-group

The producer and processor are decoupled through Kafka:

    Producer → Kafka → Processor

This allows the producer to publish an event without directly depending on the processor being available at that exact moment.

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

## Database

PostgreSQL is used for trade persistence.

The main persistence model currently revolves around the:

    trade

table.

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

## Project Roadmap

The project will evolve incrementally toward a production-style event-driven backend.

Planned areas include:

- Trade validation
- Reference-data enrichment
- Kafka consumer error handling
- Retry and Dead Letter Topics
- Idempotent processing
- Transaction management
- Trade status lifecycle
- Notification processing
- Observability and logging
- Automated testing
- Containerization
- CI/CD
- Scalable service deployment
  
## Learning Objectives

This project is being developed as a practical backend engineering exercise with emphasis on:

- Spring Boot internals
- Microservice architecture
- Kafka fundamentals and internals
- Event-driven architecture
- Serialization and deserialization
- Kafka producers and consumers
- Consumer groups and offsets
- Database persistence
- Transaction management
- Distributed-system design
- Reliability and fault tolerance
- Production-oriented backend engineering
