# Sequence Diagrams

## Trade Processing Pipeline

This document describes the major runtime interactions in the Trade Processing Pipeline. Diagrams distinguish the **current implemented flow** from **planned behavior**.

The current implementation uses:

- `trade-events` as the input Kafka topic.
- `TradeConsumerService` to consume and coordinate processing.
- Redis-backed reference-data caching and the Reference Data Service.
- `TradePersistenceService` to persist the trade and Outbox Event in one PostgreSQL transaction.
- `OutboxPublisherService` to publish pending Outbox Events to `trade-notifications`.
- `PENDING` and `PUBLISHED` as the current Outbox statuses.

Fee calculation and full Notification Service processing are planned, not yet part of the implemented end-to-end flow.

------------------------------------------------------------------------

# 1. Happy Path — Current Implementation

A client submits a trade. The producer publishes the event to Kafka, the processor enriches it, and the trade and Outbox Event are committed together. A separate publisher later sends the stored payload downstream.

``` mermaid
sequenceDiagram
    autonumber
    actor Client
    participant Producer as Trade Producer
    participant InputKafka as Kafka: trade-events
    participant Consumer as TradeConsumerService
    participant Cache as Redis Cache
    participant RefService as Reference Data Service
    participant Persistence as TradePersistenceService
    participant TradeRepo as TradeRepository
    participant OutboxRepo as OutboxEventRepository
    participant DB as PostgreSQL
    participant Publisher as OutboxPublisherService
    participant OutputKafka as Kafka: trade-notifications

    Client->>Producer: Submit trade
    Producer->>InputKafka: Publish TradeEvent
    InputKafka->>Consumer: Deliver TradeEvent
    Consumer->>TradeRepo: findByTradeId(tradeId)
    TradeRepo-->>Consumer: No existing trade
    Consumer->>Cache: Get reference data by symbol
    Cache-->>Consumer: Cache result or miss path
    opt Cache miss
        Consumer->>RefService: Request reference data
        RefService-->>Consumer: symbol, exchange, currency, sector
        Consumer->>Cache: Store reference data
    end
    Consumer->>Consumer: Build TradeEntity and OutboxEvent
    Consumer->>Persistence: persistTradeAndEvent(trade, event)
    activate Persistence
    Persistence->>TradeRepo: save(trade)
    Persistence->>OutboxRepo: save(event, status=PENDING)
    Persistence->>DB: Commit transaction
    DB-->>Persistence: Commit successful
    Persistence-->>Consumer: Return
    deactivate Persistence
    Note over Publisher,DB: Publisher runs separately on a schedule
    Publisher->>OutboxRepo: findByStatus(PENDING)
    OutboxRepo-->>Publisher: Pending events
    Publisher->>OutputKafka: Send stored payload
    OutputKafka-->>Publisher: Send acknowledged
    Publisher->>OutboxRepo: Save status=PUBLISHED and publishedAt
```

**Important:** the Outbox Publisher runs independently of the consumer transaction. HTTP acceptance, Kafka input publication, trade persistence, and downstream publication are separate stages.

------------------------------------------------------------------------

# 2. Reference Data — Redis Cache Hit

When reference data is already cached, the processor can use it without calling the Reference Data Service.

``` mermaid
sequenceDiagram
    autonumber
    participant Consumer as TradeConsumerService
    participant Cache as Redis Cache

    Consumer->>Cache: Get referenceData::AAPL
    Cache-->>Consumer: Cache hit with reference data
    Consumer->>Consumer: Enrich trade using cached values
```

This is the intended cache-hit path. It avoids a network call to the Reference Data Service for that lookup.

------------------------------------------------------------------------

# 3. Reference Data — Redis Cache Miss

When the symbol is not cached, the processor obtains the data from the Reference Data Service and caches the result.

``` mermaid
sequenceDiagram
    autonumber
    participant Consumer as TradeConsumerService
    participant Cache as Redis Cache
    participant RefService as Reference Data Service

    Consumer->>Cache: Get referenceData::AAPL
    Cache-->>Consumer: Cache miss
    Consumer->>RefService: GET /reference/AAPL
    RefService-->>Consumer: symbol, exchange, currency, sector
    Consumer->>Cache: Store reference data
    Cache-->>Consumer: Stored
    Consumer->>Consumer: Enrich trade
```

The response includes `symbol`, `exchange`, `currency`, and `sector`. The exact internal class names for the cache/client path should be kept synchronized with source code.

------------------------------------------------------------------------

# 4. Reference Data Service Failure — Retry and DLT

A temporary dependency failure can cause trade processing to fail. Spring Kafka retry-topic handling retries failed processing according to the configured `@RetryableTopic` policy. If retries are exhausted, the DLT handler receives the failed record.

``` mermaid
sequenceDiagram
    autonumber
    participant Kafka as Kafka: trade-events
    participant Consumer as TradeConsumerService
    participant RefService as Reference Data Service
    participant Retry as Spring Kafka Retry Topics
    participant DLT as Dead Letter Topic

    Kafka->>Consumer: Deliver TradeEvent
    Consumer->>RefService: Request reference data
    RefService--xConsumer: Request fails
    Consumer-->>Retry: Processing attempt fails
    Retry->>Consumer: Retry delivery
    Consumer->>RefService: Request reference data again
    alt Dependency recovers
        RefService-->>Consumer: Reference data
        Consumer->>Consumer: Continue processing
    else Retries exhausted
        Consumer-->>DLT: Failed record routed to DLT
        Note over DLT: DLT handling is implemented. Runtime configuration determines the exact topic name.
    end
```

The diagram intentionally does not prescribe exact retry-topic names or attempt timing. Those depend on the current Spring Kafka configuration. The earlier HLD’s `trade-retry-1`, `trade-retry-2`, `trade-retry-3`, and `trade-failed` names describe the original design.

------------------------------------------------------------------------

# 5. Database Transaction Failure — Trade and Outbox Roll Back Together

The trade and its Outbox Event are written through one `@Transactional` service method. If either database write fails, the transaction rolls back.

``` mermaid
sequenceDiagram
    autonumber
    participant Consumer as TradeConsumerService
    participant Persistence as TradePersistenceService
    participant TradeRepo as TradeRepository
    participant OutboxRepo as OutboxEventRepository
    participant DB as PostgreSQL

    Consumer->>Persistence: persistTradeAndEvent(trade, event)
    activate Persistence
    Persistence->>TradeRepo: save(trade)
    TradeRepo-->>Persistence: Save staged in transaction
    Persistence->>OutboxRepo: save(event)
    OutboxRepo--xPersistence: Database write fails
    Persistence->>DB: Roll back transaction
    DB-->>Persistence: Rollback complete
    Persistence-->>Consumer: Propagate failure
    deactivate Persistence
    Note over DB: Neither the trade nor its Outbox Event is committed
```

The same rollback principle applies if the trade write itself fails. This diagram represents database transaction behavior; the failed Kafka record is then subject to the consumer’s configured retry/DLT behavior.

------------------------------------------------------------------------

# 6. Duplicate Input Event — Current Trade-Level Idempotency

The consumer checks for an existing `tradeId` before creating a new trade and Outbox Event.

``` mermaid
sequenceDiagram
    autonumber
    participant Kafka as Kafka: trade-events
    participant Consumer as TradeConsumerService
    participant TradeRepo as TradeRepository

    Kafka->>Consumer: Deliver TradeEvent TR123
    Consumer->>TradeRepo: findByTradeId(TR123)
    TradeRepo-->>Consumer: Trade exists
    Consumer->>Consumer: Log duplicate and skip processing
    Note over Consumer: No new trade or Outbox Event is created by this duplicate delivery
```

The database unique constraint on `trade_id` is an additional safeguard. If concurrent processing causes two deliveries to pass the initial lookup before either insert commits, the unique constraint remains the final integrity protection; deliberate handling of that race should be considered if concurrent duplicate processing is introduced.

------------------------------------------------------------------------

# 7. Kafka Unavailable During Outbox Publication

If Kafka is unavailable when the Outbox Publisher attempts to publish, the event remains `PENDING`. A later scheduled run can try again.

``` mermaid
sequenceDiagram
    autonumber
    participant Publisher as OutboxPublisherService
    participant OutboxRepo as OutboxEventRepository
    participant Kafka as Kafka: trade-notifications

    Publisher->>OutboxRepo: findByStatus(PENDING)
    OutboxRepo-->>Publisher: Pending Outbox Event
    Publisher-xKafka: Send fails because Kafka is unavailable
    Note over Publisher,OutboxRepo: Exception is caught; event remains PENDING
    Note over Publisher,OutboxRepo: Later scheduled run
    Publisher->>OutboxRepo: findByStatus(PENDING)
    OutboxRepo-->>Publisher: Same pending event
    Publisher->>Kafka: Send stored payload
    Kafka-->>Publisher: Send acknowledged
    Publisher->>OutboxRepo: Mark PUBLISHED and set publishedAt
```

This recovery scenario has already been tested in the project. The intended guarantee is that the event remains available for retry rather than being marked published after a failed send.

------------------------------------------------------------------------

# 8. Publisher Crash After Kafka Accepts the Event

There is a small failure window between successful Kafka publication and the database update to `PUBLISHED`.

``` mermaid
sequenceDiagram
    autonumber
    participant Publisher as OutboxPublisherService
    participant Kafka as Kafka: trade-notifications
    participant OutboxRepo as OutboxEventRepository

    Publisher->>Kafka: Send event
    Kafka-->>Publisher: Send acknowledged
    Note over Publisher: Application crashes before status update
    Note over OutboxRepo: Event status remains PENDING
    Note over Publisher,Kafka: After restart, the publisher may send the same event again
```

This is an expected at-least-once delivery trade-off. The design prefers possible duplicate publication over losing an event by marking it published before Kafka confirms the send. Downstream idempotency is planned for consumers where duplicate side effects must be prevented.

------------------------------------------------------------------------

# 9. Future Notification Processing — Planned

The Notification Service is part of the target architecture, but its full consumer flow and `processed_events` persistence are not yet implemented.

``` mermaid
sequenceDiagram
    autonumber
    participant Kafka as Kafka: trade-notifications
    participant Notification as Notification Service
    participant ProcessedRepo as ProcessedEventRepository
    participant Channel as Email / SMS / Webhook

    Kafka->>Notification: Deliver downstream event
    Notification->>ProcessedRepo: Check event ID
    alt Event already processed
        ProcessedRepo-->>Notification: Event exists
        Notification->>Notification: Skip duplicate
    else Event not processed
        ProcessedRepo-->>Notification: Event not found
        Notification->>Channel: Send notification
        Channel-->>Notification: Simulated send result
        Notification->>ProcessedRepo: Record processed event
    end
```

Before implementation, the downstream event contract must define a stable event ID and the intended transaction/error behavior around sending a notification and recording the event. The exact ordering of those operations needs careful design because an external notification channel cannot normally participate in the PostgreSQL transaction.

------------------------------------------------------------------------

# 10. Future Fee Calculation — Planned

Fee calculation will be added after reference-data enrichment and before persisting the final trade.

``` mermaid
sequenceDiagram
    autonumber
    participant Consumer as TradeConsumerService
    participant FeeService as FeeCalculationService (planned)
    participant Persistence as TradePersistenceService
    participant DB as PostgreSQL

    Consumer->>Consumer: Enrich trade with reference data
    Consumer->>FeeService: Calculate fees for trade
    FeeService->>FeeService: Calculate trade value and configured fees
    FeeService-->>Consumer: Brokerage fee, tax, regulatory fee, total fees
    Consumer->>Persistence: Persist enriched trade and Outbox Event
    Persistence->>DB: Commit both writes in one transaction
```

The formulas, rate source, rounding rules, and fee-configuration schema must be finalized before implementation. This diagram describes the intended future flow, not current behavior.

------------------------------------------------------------------------

# 11. Diagram Maintenance Checklist

When implementation changes, update the relevant diagram if any of these change:

- HTTP endpoint or response contract.
- Kafka topic, message key, consumer group, or payload.
- Reference-data/cache interaction.
- Transaction boundary or persistence sequence.
- Retry/DLT configuration.
- Outbox status transitions or publication behavior.
- Downstream event schema or Notification Service flow.
- Fee calculation placement or persistence fields.

Keep the diagrams in this file consistent with `HLD.md`, `LLD.md`, `api-contracts.md`, and `database.md`.
