# Low-Level Design (LLD)

## Trade Processing Pipeline

This document describes the code-level design of the Trade Processing Pipeline. It preserves the original design intent while distinguishing implemented behavior from planned components.

**Documentation rule:** a class or capability is marked as implemented only when confirmed by the current source code. Older names and proposed components are retained as historical/planned design where useful.

------------------------------------------------------------------------

# 1. Scope and Responsibilities

The Trade Processor is responsible for the core trade-processing workflow:

1.  Consume trade events from Kafka.
2.  Detect duplicate trades.
3.  Retrieve reference data through the reference-data and Redis-cache integration.
4.  Enrich the trade with exchange, currency, and sector.
5.  Calculate fees — **planned, not yet implemented**.
6.  Persist the trade and an Outbox Event atomically.
7.  Publish pending Outbox Events asynchronously through the Outbox Publisher.
8.  Handle failed processing through Spring Kafka retry/DLT behavior.

The consumer coordinates the workflow. Business rules and persistence should be delegated to focused services where appropriate.

------------------------------------------------------------------------

# 2. Multi-Module Maven Structure

The project uses a multi-module Maven structure.

``` text
trade-processing-pipeline/
├── pom.xml
├── docker-compose.yml
├── trade-common/
├── trade-producer/
├── trade-processor/
├── reference-data-service/
├── notification-service/
└── docs/
```

| Module                   | Responsibility                                      | Status                                                               |
|--------------------------|-----------------------------------------------------|----------------------------------------------------------------------|
| `trade-common`           | Shared DTOs, enums, constants, exceptions           | Confirm exact contents against source                                |
| `trade-producer`         | Accept trade requests and publish `trade-events`    | Implemented                                                          |
| `trade-processor`        | Consume, enrich, persist, and create Outbox Events  | Core flow implemented                                                |
| `reference-data-service` | Provide reference data through an API               | API integration implemented; persistence details should match source |
| `notification-service`   | Consume downstream events and process notifications | Planned / not fully implemented                                      |

The shared module is intended to prevent duplicate definitions of event contracts across services. The common module should depend on no service module; service modules may depend on it where needed.

------------------------------------------------------------------------

# 3. Current Package and Class Structure

The original design proposed the package responsibilities below. Treat the entries marked **planned/design** as intended organization, not proof that the classes already exist.

## 3.1 `trade-processor`

``` text
trade-processor
├── consumer/
│   └── TradeConsumerService                 [implemented]
├── service/
│   └── TradePersistenceService               [implemented]
├── outbox/
│   └── OutboxPublisherService                [implemented]
├── entity/
│   ├── TradeEntity                           [implemented]
│   └── OutboxEvent                           [implemented]
├── repository/
│   ├── TradeRepository                       [implemented]
│   └── OutboxEventRepository                 [implemented]
├── enums/
│   └── OutboxStatus                          [implemented]
├── cache / client / config / exception/
│   └── Verify exact classes and package paths in source
├── validator/
│   └── TradeValidator                        [planned]
├── mapper/
│   └── TradeMapper                           [planned]
└── service/
    └── FeeCalculationService                 [planned]
```

The exact physical package paths should be confirmed against the current repository before they are treated as authoritative. The tree above describes responsibilities and known classes; it is not intended to invent missing classes.

## 3.2 Other modules

The original design proposes the following responsibilities:

- **`trade-producer`** — REST controller, request handling, and Kafka producer.
- **`reference-data-service`** — reference-data controller, service, and its persistence layer where implemented.
- **`notification-service`** — downstream consumer, notification service, and idempotency persistence; planned until verified in code.
- **`trade-common`** — shared event DTOs, enums, constants, and exceptions; exact contents to be verified.

------------------------------------------------------------------------

# 4. Current End-to-End Processing Flow

``` text
Trade Producer
      |
      v
Kafka: trade-events
      |
      v
TradeConsumerService
      |
      v
Duplicate check by tradeId
      |
      v
Reference-data lookup / Redis cache
      |
      v
Build TradeEntity + OutboxEvent
      |
      v
TradePersistenceService
      |
      | @Transactional
      v
PostgreSQL
      |
      v
OutboxPublisherService
      |
      v
Kafka: trade-notifications
```

Fee calculation will be added between reference-data enrichment and construction/persistence of the final trade.

------------------------------------------------------------------------

# 5. Class Responsibilities

## 5.1 `TradeConsumerService` — implemented

The consumer orchestrates processing of a Kafka trade event. Current responsibilities include:

- Receive a `TradeEvent`.
- Log the event.
- Check whether the `tradeId` already exists.
- Retrieve reference data.
- Construct a `TradeEntity`.
- Serialize the event payload for the Outbox.
- Construct an `OutboxEvent`.
- Delegate the database writes to `TradePersistenceService`.

The consumer uses Spring Kafka retry handling through `@RetryableTopic` and a `@DltHandler`.

As the application grows, new business rules such as fee calculation should be delegated to a focused service rather than added as more responsibilities inside the consumer.

## 5.2 `TradePersistenceService` — implemented

**Responsibility:** persist the trade and Outbox Event in one PostgreSQL transaction.

Conceptually:

``` java
@Transactional
public void persistTradeAndEvent(
        TradeEntity tradeEntity,
        OutboxEvent outboxEvent) {

    tradeRepository.save(tradeEntity);
    outboxEventRepository.save(outboxEvent);
}
```

Both writes commit together or roll back together. Kafka publishing does not occur inside this database transaction.

## 5.3 `OutboxPublisherService` — implemented

**Responsibility:** publish pending Outbox Events to Kafka.

The current implementation:

1.  Runs on a schedule.
2.  Finds Outbox Events with status `PENDING`.
3.  Sends each event to the topic stored on the Outbox Event.
4.  Waits for the Kafka send result.
5.  Marks the event `PUBLISHED` and sets `publishedAt` after a successful send.
6.  Leaves the event `PENDING` if publishing fails, so a later run can retry.

The current scheduler uses a 60-second initial delay and a 60-second fixed delay.

The publisher provides at-least-once publication, not exactly-once publication. A crash after Kafka accepts the event but before the database status update can result in the event being published again.

## 5.4 Reference-data integration

The Trade Processor retrieves symbol reference data through the reference-data/cache integration.

The current response contract includes `symbol`:

``` json
{
  "symbol": "AAPL",
  "exchange": "NASDAQ",
  "currency": "USD",
  "sector": "TECH"
}
```

This response shape should be kept consistent in the DTO and API documentation.

## 5.5 `FeeCalculationService` — planned

A dedicated fee-calculation service is part of the design but has not yet been implemented.

Its planned responsibilities are:

- Calculate trade value.
- Calculate brokerage fee.
- Calculate tax.
- Calculate regulatory fee.
- Calculate total fees.

Conceptual formulas:

``` text
tradeValue       = price × quantity
brokerageFee     = tradeValue × brokerageRate
tax              = tradeValue × taxRate
regulatoryFee    = tradeValue × regulatoryRate
totalFees        = brokerageFee + tax + regulatoryFee
```

Rates, precision/rounding rules, and fee-configuration persistence must be designed before implementation. These formulas are conceptual, not a claim that the current code performs these calculations.

------------------------------------------------------------------------

# 6. Class Interaction Diagram

The diagram below shows the current known interactions. The exact method names for reference-data retrieval should be taken from the source when the API/client classes are verified.

``` text
Kafka: trade-events
        |
        v
TradeConsumerService
        |
        +----> TradeRepository
        |          |
        |          +----> Check existing tradeId
        |
        +----> Reference-data / Redis integration
        |
        +----> Build TradeEntity
        |
        +----> Build OutboxEvent
                    |
                    v
          TradePersistenceService
                    |
             +------+------+
             |             |
             v             v
      TradeRepository  OutboxEventRepository
             |             |
             +------+------+
                    |
                    v
                PostgreSQL


OutboxPublisherService
        |
        v
OutboxEventRepository
        |
        v
Find PENDING events
        |
        v
KafkaTemplate
        |
        v
Kafka: trade-notifications
        |
        v
Update event to PUBLISHED after send success
```

------------------------------------------------------------------------

# 7. Kafka Layer

## 7.1 Input topic

``` text
Topic: trade-events
Producer: trade-producer
Consumer: trade-processor
Consumer group: trade-processor-group
Message key: tradeId
```

Using `tradeId` as the message key is intended to route events for the same trade to the same partition. The current local setup uses one partition for `trade-events`; multi-partition configuration is a scaling target.

## 7.2 Downstream topic

The current Outbox configuration publishes to:

``` text
trade-notifications
```

The older design used `trade-processed`. That name is retained as historical design terminology; the current implementation uses `trade-notifications`.

The Notification Service and other downstream consumers must be treated as planned until their consumption flow is implemented and verified.

## 7.3 Retry and Dead Letter Topic handling

The current consumer uses Spring Kafka’s `@RetryableTopic` and `@DltHandler`.

The original design described explicit topics such as:

``` text
trade-retry-1
trade-retry-2
trade-retry-3
trade-failed
```

These names describe the original design concept. Do not assume every topic is manually configured with exactly these names in the current application.

------------------------------------------------------------------------

# 8. Idempotency

## 8.1 Trade-level idempotency — implemented

The consumer checks whether a trade already exists by `tradeId`.

``` text
Incoming TradeEvent
        |
        v
Lookup by tradeId
        |
     +--+--+
     |     |
   exists  absent
     |       |
    skip   continue
```

The database also enforces uniqueness on `trade_id`.

These protections are complementary:

- The application-level check avoids duplicate work in the normal case.
- The database unique constraint protects data integrity if concurrent or unexpected duplicate writes occur.

**Concurrency note:** two consumers could theoretically check for the same trade before either insert commits. The unique constraint remains the final integrity safeguard. If concurrent duplicate processing is introduced, the application should handle a unique-constraint violation deliberately.

## 8.2 Downstream event idempotency — planned

The original design includes a `processed_events` table for consumers such as Notification Service:

``` text
processed_events
-------------------------
event_id       PRIMARY KEY
processed_at
```

This table and its downstream workflow are planned, not part of the current Trade Processor implementation.

------------------------------------------------------------------------

# 9. Persistence Layer

## 9.1 `TradeEntity`

Maps to the current `trade` table.

Current fields include:

``` text
id
trade_id          UNIQUE
symbol
side
quantity
price
exchange
currency
sector
brokerage_fee
tax
status
created_at
updated_at
```

The current entity includes `brokerage_fee` and `tax`. The fee-calculation milestone is expected to add `regulatory_fee` and `total_fees`, subject to the final schema decision.

## 9.2 `OutboxEvent`

Maps to the current `outbox_event` table.

Current fields include:

``` text
id
event_type
aggregate_id
topic
payload
status
created_at
published_at
```

Current status values:

``` text
PENDING
PUBLISHED
```

The payload is stored as text in the current implementation.

The older design used `OutboxEventEntity`, `outbox_events`, and `NEW`/`SENT`. Those are historical names and should not be used to describe the current implementation.

## 9.3 Repository responsibilities

- `TradeRepository` — trade persistence and lookup, including lookup by `tradeId`.
- `OutboxEventRepository` — Outbox Event persistence and lookup by status.

Repositories should focus on data access. Processing orchestration belongs in services.

## 9.4 Reference-data persistence — verify against source

The original design describes:

``` text
reference_data
-------------------------
symbol       PRIMARY KEY
exchange
currency
sector
```

The Reference Data API response includes `symbol`, `exchange`, `currency`, and `sector`. An API response alone does not prove that a particular database table exists. Keep the persistence status aligned with the actual `reference-data-service` source.

------------------------------------------------------------------------

# 10. Transaction Boundary

The transaction boundary is deliberately narrow:

``` text
TradePersistenceService.persistTradeAndEvent()
        |
        v
@Transactional
        |
        +---- tradeRepository.save(trade)
        |
        +---- outboxEventRepository.save(event)
        |
        v
Commit or roll back both writes
```

The Outbox Publisher runs separately. Kafka availability does not determine whether the trade and Outbox Event can be committed to PostgreSQL.

This is the core guarantee provided by the Transactional Outbox pattern in this application.

------------------------------------------------------------------------

# 11. Redis Cache Layer

Redis caches reference data.

``` text
Request reference data for symbol
        |
        v
      Redis
        |
     +--+--+
     |     |
    HIT   MISS
     |     |
     |     v
     | Reference Data Service
     |     |
     |     v
     | Cache response
     |     |
     +-----+
        |
        v
Return reference data
```

Current cache name:

``` text
referenceData
```

Example cache key:

``` text
referenceData::AAPL
```

Cache serialization and key conventions should remain aligned with the actual cache configuration.

------------------------------------------------------------------------

# 12. DTOs and API Contracts

## 12.1 `TradeEvent`

The trade event consumed from Kafka contains fields such as:

``` json
{
  "tradeId": "TR123",
  "symbol": "AAPL",
  "quantity": 100,
  "price": 220,
  "side": "BUY"
}
```

The actual shared DTO in source is authoritative if it differs from this example.

## 12.2 `ReferenceDataResponse`

The current response includes:

``` json
{
  "symbol": "AAPL",
  "exchange": "NASDAQ",
  "currency": "USD",
  "sector": "TECH"
}
```

The `symbol` field was added during implementation and must be retained in the LLD and API documentation.

## 12.3 Outbox payload contract

The current consumer serializes a `TradeEvent` payload for storage in the Outbox Event. The publisher sends the stored payload to the topic configured on that event.

The original design proposed a separate `ProcessedTradeEvent` published to `trade-processed`. Do not assume that older DTO/topic contract is the current one. The exact downstream event contract should be finalized and documented in `api-contracts.md` before implementing the Notification Service.

------------------------------------------------------------------------

# 13. Configuration and Utility Responsibilities

The original design proposed the following configuration and utility responsibilities. Verify each class name and package against the source before marking it implemented.

| Concern                               | Responsibility                                                         | Status                                           |
|---------------------------------------|------------------------------------------------------------------------|--------------------------------------------------|
| Kafka producer/consumer configuration | Producer factory, consumer factory, `KafkaTemplate` and listener setup | Configuration exists; verify exact classes       |
| Redis configuration                   | Redis connection, template/cache serialization                         | Configuration exists; verify exact classes       |
| Scheduling                            | Enable and configure scheduled Outbox publication                      | Outbox publisher runs on a fixed delay           |
| JSON serialization                    | Serialize event payloads for the Outbox                                | Used by current consumer                         |
| Shared constants                      | Kafka topic names and cache keys                                       | Verify exact definitions                         |
| Exception hierarchy                   | Business and processing exceptions                                     | Preserve planned design until source is verified |

The original LLD also proposed a `JsonUtil` helper and shared constants such as `KafkaTopics` and `CacheKeys`. These should remain planned unless the corresponding classes are present in the repository.

------------------------------------------------------------------------

# 14. Error Handling and Failure Semantics

## 14.1 Consumer processing failure

Spring Kafka retry handling retries failed processing according to the configured `@RetryableTopic` settings. After retries are exhausted, the DLT handler receives the failed message.

## 14.2 Database transaction failure

If either the trade write or Outbox Event write fails within `TradePersistenceService`, the transaction rolls back both writes.

## 14.3 Kafka unavailable during Outbox publication

The publisher catches publication failures and leaves the Outbox Event as `PENDING`. A later scheduled run can retry it.

## 14.4 Duplicate input trade

The application checks for an existing `tradeId` and skips a duplicate. The unique database constraint is an additional integrity safeguard.

## 14.5 Duplicate downstream publication

A publisher crash after a successful Kafka send but before the database status update may result in republishing. Downstream idempotency is a planned safeguard for consumers where duplicates would have side effects.

## 14.6 Validation and exception hierarchy

The original design proposed a dedicated `TradeValidator` and a business-exception hierarchy. Their exact implementation and retry/DLT classification must be verified against the current source. Do not document them as existing until implemented.

------------------------------------------------------------------------

# 15. Planned Components

The following components appear in the original design but are not represented as implemented in this LLD unless verified in the current repository:

- Dedicated `TradeValidator`.
- Separate `TradeService` business-orchestration class.
- `FeeCalculationService`.
- `TradeMapper`.
- `OutboxService` abstraction.
- `ProcessedEventEntity` and `ProcessedEventRepository`.
- Notification consumer and notification-processing service.
- Persistent downstream event idempotency.
- Analytics and Audit consumers.
- Fee-configuration persistence.
- Any proposed `JsonUtil`, `KafkaTopics`, or `CacheKeys` classes not yet present in source.

The purpose of retaining these items is to show the intended design direction without confusing it with the current implementation.

------------------------------------------------------------------------

# 16. End-to-End Processing Sequence

## 16.1 Current implemented path

``` text
1. Client submits a trade
2. Trade Producer publishes a TradeEvent to trade-events
3. Trade Processor consumes the event
4. Consumer checks whether tradeId already exists
5. Consumer obtains reference data through the cache/service integration
6. Consumer constructs TradeEntity and OutboxEvent
7. TradePersistenceService saves both in one transaction
8. OutboxPublisherService finds PENDING events
9. Publisher sends the stored payload to trade-notifications
10. On successful send, the publisher marks the event PUBLISHED
```

Fee calculation is a planned step between reference-data enrichment and persistence.

## 16.2 Planned future path

``` text
TradeEvent
    |
    v
Duplicate check
    |
    v
Reference-data enrichment
    |
    v
FeeCalculationService [planned]
    |
    v
Save Trade + Outbox Event atomically
    |
    v
Outbox Publisher
    |
    v
Kafka downstream event
    |
    v
Notification Service [planned]
    |
    v
processed_events idempotency [planned]
```

------------------------------------------------------------------------

# 17. Development Principles

- Keep the consumer focused on orchestration.
- Keep repositories focused on data access.
- Keep trade and Outbox writes in one database transaction.
- Keep Kafka publication outside that database transaction.
- Use `tradeId` as the trade-level idempotency key.
- Treat the database unique constraint as the final trade-integrity safeguard.
- Keep shared event contracts consistent across modules.
- Mark planned components explicitly.
- Update the LLD when implementation changes.

------------------------------------------------------------------------

# 18. LLD Maintenance Checklist

When a feature is implemented:

1.  Update the relevant class and package descriptions.
2.  Update the current processing and interaction diagrams.
3.  Update DTO and API contracts.
4.  Update database entities and repository responsibilities.
5.  Update Kafka topics, event payloads, and status values.
6.  Move the feature from Planned to Implemented.
7.  Ensure the LLD, HLD, API contracts, sequence diagrams, and database documentation remain consistent.
