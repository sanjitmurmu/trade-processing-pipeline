# Trade Processing Pipeline

A backend microservice application built with **Java, Spring Boot, Apache Kafka, and PostgreSQL** for processing financial trade events asynchronously.

The project is designed as a hands-on backend engineering project to demonstrate event-driven architecture, Kafka-based communication, persistence, microservices, and scalable backend design.

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
                                    │ Publish TradeEvent
                                    ▼
                         ┌─────────────────────┐
                         │   Apache Kafka      │
                         │                     │
                         │   trade-events      │
                         └──────────┬──────────┘
                                    │
                                    │ Consume TradeEvent
                                    ▼
                         ┌─────────────────────┐
                         │   Trade Processor   │
                         │    Spring Boot      │
                         └──────────┬──────────┘
                                    │
                                    │ Persist Trade
                                    ▼
                         ┌─────────────────────┐
                         │    PostgreSQL       │
                         │                     │
                         │     trade table     │
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
- Maven
- Docker / Docker Compose
- Lombok

## Current Processing Flow

The current end-to-end flow is:
    
    REST Request
         ↓
    Trade Producer
         ↓
    JSON → TradeEvent
         ↓
    Kafka Producer
         ↓
    Kafka Topic: trade-events
         ↓
    Kafka Consumer
         ↓
    Trade Processor
         ↓
    TradeEntity
         ↓
    PostgreSQL

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

trade-processor listens to the trade-events topic using the consumer group:

    trade-processor-group

The Kafka message is deserialized back into a TradeEvent.

4. Persistence

The processor maps the event to a TradeEntity and stores it in PostgreSQL.

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
