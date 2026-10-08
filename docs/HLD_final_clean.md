# High Level Design

## Trade Processing Pipeline

This document describes the architecture of the Trade Processing Pipeline, the reasoning behind its evolution, the current implementation, and the planned components.

The design is intentionally evolutionary. Some sections describe capabilities that are already implemented, while others describe components that are planned as part of the final application.

------------------------------------------------------------------------

# 1. System Overview

The Trade Processing Pipeline is an event-driven system that:

- Receives trade events from upstream systems.
- Validates trade data.
- Enriches trades with reference data.
- Calculates fees and taxes.
- Persists processed trades.
- Publishes downstream events.
- Handles transient and permanent failures.
- Prevents duplicate trade processing.
- Supports downstream notification and other consumers.

At a high level:

``` text
                         +------------------+
                         |  Trade Producer  |
                         +--------+---------+
                                  |
                                  | trade-events
                                  v
                         +------------------+
                         |      Kafka       |
                         +--------+---------+
                                  |
                                  v
                         +------------------+
                         |  Trade Processor |
                         +---+----------+---+
                             |          |
                    Reference Data      | Redis
                             |          |
                             v          |
                    +----------------+  |
                    | Reference Data |<-+
                    |    Service     |
                    +-------+--------+
                            |
                            v
                    +------------------+
                    |    PostgreSQL    |
                    |                  |
                    | trade            |
                    | outbox_event     |
                    | reference_data   |
                    | processed_events |
                    +--------+---------+
                             |
                             v
                    +------------------+
                    | Outbox Publisher |
                    +--------+---------+
                             |
                             v
                    +------------------+
                    |      Kafka       |
                    +--------+---------+
                             |
                             v
                    +--------------------+
                    | Notification       |
                    | Service            |
                    +--------------------+
```

------------------------------------------------------------------------

# 2. Major Components

## 2.1 Trade Producer

The Trade Producer simulates an upstream trading system.

Responsibilities:

- Accept trade requests.
- Build trade events.
- Publish events to Kafka.

Input topic:

``` text
trade-events
```

## 2.2 Kafka

Kafka is the event backbone of the system.

It decouples:

- Trade Producer from Trade Processor.
- Trade Processor from downstream consumers.

The trade event uses `tradeId` as the Kafka key so events for the same trade can be routed to the same partition and maintain ordering within that partition.

## 2.3 Trade Processor

The Trade Processor is the core processing component.

At a high level it:

1.  Consumes trade events.
2.  Detects duplicate trades.
3.  Validates trade data.
4.  Retrieves reference data.
5.  Enriches the trade.
6.  Calculates fees and taxes.
7.  Persists the trade.
8.  Persists an Outbox Event in the same database transaction.
9.  Allows the Outbox Publisher to publish the downstream event.

Fee calculation is currently planned and is the next major development milestone.

## 2.4 Reference Data Service

The Reference Data Service provides reference information for a symbol.

Example endpoint:

``` text
GET /reference/AAPL
```

The current response includes the symbol:

``` json
{
  "symbol": "AAPL",
  "exchange": "NASDAQ",
  "currency": "USD",
  "sector": "TECH"
}
```

The original architecture also defines persistent reference data:

``` text
reference_data
--------------------------------
symbol       PRIMARY KEY
exchange
currency
sector
```

## 2.5 PostgreSQL

PostgreSQL is the primary relational database.

The architecture contains four logical data areas:

- `trade`
- `outbox_event`
- `reference_data`
- `processed_events`

Not all of these are fully implemented yet.

PostgreSQL is also the transaction boundary for the Trade + Outbox operation.

## 2.6 Redis

Redis caches reference data to reduce repeated calls to the Reference Data Service.

Example cache entry:

``` text
Key:
AAPL

Value:
{
  "symbol": "AAPL",
  "exchange": "NASDAQ",
  "currency": "USD",
  "sector": "TECH"
}
```

## 2.7 Notification Service

The architecture includes a Notification Service as a downstream consumer.

It is intended to consume downstream trade events and simulate:

- Email.
- SMS.
- Webhook.

The full downstream processing implementation is still planned.

------------------------------------------------------------------------

# 3. Initial Architecture

The initial design was intentionally simple:

``` text
Client
  |
  v
Trade Producer
  |
  v
Kafka: trade-events
  |
  v
Trade Processor
  |
  +----> Reference Data Service
  |
  v
PostgreSQL
```

The Trade Processor consumed the event, obtained reference data, enriched the trade, and persisted the result.

This provided the foundation for the later reliability and performance improvements.

------------------------------------------------------------------------

# 4. HLD V1 — Redis Caching

## 4.1 Problem

The Trade Processor needs reference information for each symbol.

Without caching:

``` text
1 million trades
       |
       v
1 million REST calls
       |
       v
Reference Data Service
```

This creates unnecessary network traffic and latency.

## 4.2 Redis Solution

Redis was introduced as a cache:

``` text
Trade Processor
      |
      v
    Redis
      |
   +--+--+
   |     |
  HIT   MISS
   |     |
   |     v
   |  Reference Data Service
   |       |
   |       v
   |     Redis
   |       |
   +-------+
       |
       v
Reference Data
```

The original HLD used illustrative latency figures to explain the motivation for caching. Those figures are design examples, not benchmark results.

------------------------------------------------------------------------

# 5. HLD V2 — Retry and Dead Letter Handling

## 5.1 Transient Failures

Reference Data Service failures may be temporary.

The system should therefore retry failures that may recover.

## 5.2 Exponential Backoff

The original design illustrates:

``` text
Attempt 1 -> failure
    |
  wait 1s
    |
Attempt 2 -> failure
    |
  wait 2s
    |
Attempt 3 -> failure
    |
  wait 4s
    |
Attempt 4 -> failure
    |
    v
   DLQ
```

Exponential backoff prevents continuously hammering a failing dependency.

## 5.3 Permanent Failures

Some failures are not transient.

Example:

``` json
{
  "tradeId": "TR456",
  "quantity": -10
}
```

Retrying an invalid quantity will not make the message valid.

Such failures should ultimately be routed to a Dead Letter mechanism.

## 5.4 Original Retry-Topic Design

The original HLD described:

``` text
trade-events
     |
     v
trade-retry-1
     |
     v
trade-retry-2
     |
     v
trade-retry-3
     |
     v
trade-failed
```

## 5.5 Current Implementation

The current Trade Processor uses Spring Kafka’s `@RetryableTopic` and `@DltHandler`.

Therefore, the manually named retry topics above represent the architectural concept from the original design, not the exact current topic configuration.

------------------------------------------------------------------------

# 6. HLD V3 — Transactional Outbox

## 6.1 The Consistency Problem

A naive implementation might do:

``` text
Save Trade
    |
    v
Publish Kafka Event
```

If the database succeeds but Kafka fails:

``` text
PostgreSQL -> SUCCESS
Kafka      -> FAILURE
```

the trade exists without a downstream event.

If Kafka succeeds but the database fails:

``` text
Kafka      -> SUCCESS
PostgreSQL -> FAILURE
```

downstream systems receive an event for a trade that was not persisted.

## 6.2 Why Not Two-Phase Commit?

A distributed transaction across Kafka and PostgreSQL would add complexity and coordination overhead.

The selected design is the Transactional Outbox Pattern.

## 6.3 Transactional Outbox Solution

Trade persistence and Outbox Event persistence happen inside one database transaction:

``` text
             PostgreSQL Transaction
             +----------------------+
             | Save Trade           |
             |       AND            |
             | Save Outbox Event    |
             +----------------------+
                       |
                    COMMIT
                       |
             Both succeed together
```

If the transaction fails:

``` text
Trade save       -> ROLLBACK
Outbox save      -> ROLLBACK
```

## 6.4 Outbox Publisher

The Outbox Publisher periodically finds pending events and publishes them to Kafka.

``` text
PostgreSQL
    |
    v
outbox_event
    |
status=PENDING
    |
    v
Outbox Publisher
    |
    v
Kafka
```

If Kafka is unavailable:

``` text
PENDING
   |
   X Kafka unavailable
   |
   v
Remain PENDING
   |
   v
Retry on next scheduled run
```

If publishing succeeds:

``` text
PENDING
   |
   v
Kafka send succeeds
   |
   v
PUBLISHED
```

The current publisher waits for the Kafka send result before marking the event `PUBLISHED`.

This provides an at-least-once publishing model. A crash after Kafka accepts the event but before the database status is updated can cause the event to be published again.

------------------------------------------------------------------------

# 7. HLD V4 — Idempotent Processing

## 7.1 Why Duplicates Can Happen

Kafka commonly operates with at-least-once processing semantics.

Example:

``` text
Kafka
  |
  v
TR123 received
  |
  v
Trade processed
  |
  X
Consumer crashes before offset commit
  |
  v
Kafka redelivers TR123
```

## 7.2 Trade ID as Idempotency Key

`tradeId` is the natural idempotency key.

``` text
Does tradeId already exist?
          |
       +--+--+
       |     |
      YES    NO
       |     |
      Skip  Process
```

The current application performs an application-level duplicate check.

The database also enforces uniqueness on `trade_id`.

Therefore:

``` text
Application duplicate check
          +
Database UNIQUE constraint
```

protect the trade table from duplicate processing.

## 7.3 Downstream Idempotency

The original architecture also defines downstream idempotency through:

``` text
processed_events
--------------------------------
event_id
processed_at
```

A downstream consumer can check whether an event was already processed before performing an irreversible action such as sending a notification.

This is planned and is not yet fully implemented.

------------------------------------------------------------------------

# 8. HLD V5 — Database and Event Design

The original design contains four logical database areas:

1.  Trade Table
2.  Outbox Table
3.  Processed Events Table
4.  Reference Data Table

## 8.1 Trade Table

Current logical structure:

``` text
trade
--------------------------------
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

The current implementation already contains `brokerage_fee` and `tax`.

The fee-calculation milestone will add:

``` text
regulatory_fee
total_fees
```

## 8.2 Outbox Table

Current implementation:

``` text
outbox_event
--------------------------------
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

The current payload is stored as text.

The older HLD used `outbox_events`, `NEW/SENT`, and a more detailed conceptual schema. Those describe the earlier design; the current implementation above is authoritative.

## 8.3 Processed Events Table

The target downstream idempotency table is:

``` text
processed_events
--------------------------------
event_id        PRIMARY KEY
processed_at
```

Purpose:

- Prevent duplicate downstream processing.
- Support Notification Service idempotency.
- Provide a reusable pattern for future Audit and Analytics consumers.

This is planned.

## 8.4 Reference Data Table

The architecture defines:

``` text
reference_data
--------------------------------
symbol          PRIMARY KEY
exchange
currency
sector
```

Example:

| symbol | exchange | currency | sector |
|--------|----------|----------|--------|
| AAPL   | NASDAQ   | USD      | TECH   |
| MSFT   | NASDAQ   | USD      | TECH   |

The Reference Data Service is intended to use this data for requests such as:

``` text
GET /reference/AAPL
```

The current API response includes `symbol`.

------------------------------------------------------------------------

# 9. Kafka Topic Design

## 9.1 Input Topic

``` text
trade-events
```

Producer:

``` text
Trade Producer
```

Consumer:

``` text
Trade Processor
```

Key:

``` text
tradeId
```

## 9.2 Current Downstream Topic

The current Outbox configuration publishes to:

``` text
trade-notifications
```

The original HLD used:

``` text
trade-processed
```

`trade-processed` is historical design terminology; `trade-notifications` is the current implementation.

The target architecture can support downstream consumers such as:

``` text
Notification Service
Analytics Service
Audit Service
```

## 9.3 Retry and DLQ

The original architecture described:

``` text
trade-retry-1
trade-retry-2
trade-retry-3
trade-failed
```

The current implementation delegates retry handling to Spring Kafka’s retry mechanism.

------------------------------------------------------------------------

# 10. Kafka Partitioning and Event Ordering

The original design proposed:

``` text
trade-events      -> 3 partitions
trade-processed   -> 3 partitions
trade-failed      -> 1 partition
```

The reason for multiple partitions is horizontal processing and parallelism.

The Kafka key is:

``` text
tradeId
```

Therefore:

``` text
TR123 event 1
TR123 event 2
TR123 event 3
```

are routed to the same partition.

This preserves ordering for events belonging to the same trade.

The current local development setup uses a single partition for `trade-events`. Partition count can be increased when the system is scaled and benchmarked.

------------------------------------------------------------------------

# 11. Reference Data Architecture

The intended Reference Data flow is:

``` text
Trade Processor
      |
      v
Redis
      |
      +---- HIT ----> Return reference data
      |
      +---- MISS
             |
             v
     Reference Data Service
             |
             v
       reference_data
             |
             v
       Return response
             |
             v
       Store in Redis
```

Current response:

``` json
{
  "symbol": "AAPL",
  "exchange": "NASDAQ",
  "currency": "USD",
  "sector": "TECH"
}
```

The `symbol` field is part of the implemented response contract.

------------------------------------------------------------------------

# 12. Notification Service and Downstream Idempotency

The planned downstream flow is:

``` text
Outbox Publisher
      |
      v
Kafka: trade-notifications
      |
      v
Notification Service
      |
      v
processed_events
      |
   +--+--+
   |     |
  YES    NO
   |     |
 Ignore  Send notification
         |
         v
   Save processed event
```

The Notification Service may simulate:

- Email.
- SMS.
- Webhook.

The `processed_events` table protects against duplicate notification processing.

This component is planned and is not yet fully implemented.

------------------------------------------------------------------------

# 13. Fee Calculation Architecture

Fee calculation is a required capability but has not yet been implemented.

The planned flow is:

``` text
Trade Processor
      |
      v
Fee Calculation Service
      |
      v
Trade Value
      |
      +----> Brokerage Fee
      |
      +----> Tax
      |
      +----> Regulatory Fee
      |
      v
Total Fees
```

Conceptual formulas:

``` text
tradeValue       = price × quantity

brokerageFee     = tradeValue × brokerageRate
tax              = tradeValue × taxRate
regulatoryFee    = tradeValue × regulatoryRate

totalFees        = brokerageFee
                 + tax
                 + regulatoryFee
```

The rates should be configurable rather than hardcoded into business logic.

A possible future configuration model is:

``` text
fee_configuration
--------------------------------
id
fee_type
rate
active
effective_from
effective_to
```

The exact rate configuration, rounding rules, effective-date behavior, and database model will be decided before implementation.

------------------------------------------------------------------------

# 14. Failure Scenarios

## 14.1 Reference Data Service Temporarily Unavailable

``` text
Trade Processor
      |
      X
Reference Data Service
      |
      v
Retry
      |
      +---- SUCCESS ----> Continue
      |
      +---- repeated failure ----> DLT
```

## 14.2 Invalid Trade

``` text
Trade Event
    |
    v
Validation
    |
    X
Invalid
    |
    v
DLT
```

## 14.3 Kafka Down During Outbox Publishing

``` text
Trade + Outbox Event
        |
        v
Single DB Transaction
        |
        v
      COMMIT
        |
        v
Outbox Publisher
        |
        X
      Kafka
        |
        v
Outbox remains PENDING
        |
        v
Retry later
        |
        v
Kafka available
        |
        v
PUBLISHED
```

## 14.4 Duplicate Kafka Delivery

``` text
Kafka
  |
  v
TR123
  |
  v
Trade Processor
  |
  v
Trade already exists?
  |
 YES
  |
  v
Skip processing
```

## 14.5 Publisher Crash After Kafka Publish

There is a small failure window:

``` text
Outbox PENDING
      |
      v
Kafka publish succeeds
      |
      X
Application crashes
      |
      v
Database still says PENDING
```

After restart, the publisher may publish the event again.

This is an expected at-least-once behavior. The architecture prioritizes avoiding lost events and uses downstream idempotency where required.

------------------------------------------------------------------------

# 15. Current End-to-End Architecture

The current implemented flow is:

``` text
Client
  |
  v
Trade Producer
  |
  | trade event
  v
Kafka: trade-events
  |
  v
Trade Processor
  |
  +----> Redis
  |         |
  |         +---- HIT -> reference data
  |         |
  |         +---- MISS -> Reference Data Service
  |
  v
Trade + Outbox Event
  |
  | PostgreSQL transaction
  v
PostgreSQL
  |
  v
Outbox Publisher
  |
  v
Kafka: trade-notifications
```

The next planned addition is:

``` text
Reference Data Enrichment
          |
          v
   Fee Calculation
          |
          v
Trade + Outbox Event
```

------------------------------------------------------------------------

# 16. Target Downstream Architecture

As additional services are implemented:

``` text
                         +----------------------+
                         |    Trade Producer    |
                         +----------+-----------+
                                    |
                                    v
                              Kafka trade-events
                                    |
                                    v
                         +----------------------+
                         |    Trade Processor   |
                         +----+------------+----+
                              |            |
                         Redis / Ref       |
                              |            |
                              +-------> PostgreSQL
                                           |
                                  +--------+--------+
                                  |                 |
                               trade          outbox_event
                                                    |
                                                    v
                                             Outbox Publisher
                                                    |
                                                    v
                                         Kafka trade-notifications
                                                    |
                              +---------------------+------------------+
                              |                     |                  |
                              v                     v                  v
                       Notification            Analytics            Audit
                         Service                Service             Service
                              |
                              v
                       processed_events
```

This is the target architecture, not a claim that every downstream component is already implemented.

------------------------------------------------------------------------

# 17. Important Processing Sequences

## 17.1 Happy Path

``` text
Trade Producer
      |
      v
Kafka: trade-events
      |
      v
Trade Processor
      |
      v
Duplicate Check
      |
      v
Reference Data
      |
      v
Fee Calculation [planned]
      |
      v
Save Trade + Outbox Event
      |
      v
Commit Transaction
      |
      v
Outbox Publisher
      |
      v
Kafka: trade-notifications
      |
      v
Downstream Consumer
```

## 17.2 Redis Cache Hit

``` text
Trade Processor
      |
      v
Redis: get(AAPL)
      |
      v
HIT
      |
      v
Return reference data
      |
      v
Continue processing
```

## 17.3 Redis Cache Miss

``` text
Trade Processor
      |
      v
Redis: get(AAPL)
      |
      v
MISS
      |
      v
Reference Data Service
      |
      v
Reference Data
      |
      +------> Store in Redis
      |
      v
Trade Processor
```

## 17.4 Duplicate Delivery

``` text
Kafka
  |
  v
TR123
  |
  v
Trade Processor
  |
  v
Process and persist
  |
  X
Crash before offset commit
  |
  v
Kafka redelivers TR123
  |
  v
findByTradeId(TR123)
  |
  v
Already exists
  |
  v
Skip
```

## 17.5 Kafka Down During Publishing

``` text
Trade Processor
      |
      v
Save Trade + Outbox Event
      |
      v
COMMIT
      |
      v
Outbox Publisher
      |
      X
Kafka
      |
      v
PENDING remains
      |
      v
Next scheduled run
      |
      v
Kafka available
      |
      v
Publish
      |
      v
Mark PUBLISHED
```

------------------------------------------------------------------------

# 18. Architecture Evolution Summary

| Version | Problem                       | Solution                                           |
|---------|-------------------------------|----------------------------------------------------|
| Initial | Basic trade processing        | Kafka + Trade Processor + PostgreSQL               |
| V1      | Repeated reference-data calls | Redis Cache                                        |
| V2      | Transient/permanent failures  | Retry + DLQ                                        |
| V3      | DB/Kafka consistency          | Transactional Outbox                               |
| V4      | Duplicate processing          | Trade ID idempotency                               |
| V5      | Broader data/event model      | Trade + Outbox + Reference Data + Processed Events |
| Next    | Fee calculation               | Fee Calculation Service + configurable rates       |
| Future  | Downstream processing         | Notification + Analytics + Audit consumers         |

------------------------------------------------------------------------

# 19. Current vs Planned

## Implemented

- Trade Producer.
- Kafka trade ingestion.
- Trade Processor.
- Reference Data Service integration.
- Current Reference Data response including `symbol`.
- Redis reference-data caching.
- PostgreSQL trade persistence.
- Transactional Outbox.
- Outbox Publisher.
- Retry handling using Spring Kafka.
- DLT handling.
- Application-level duplicate trade detection.
- Unique `trade_id` database constraint.

## Planned / In Progress

- Fee Calculation Service.
- Configurable fee rates.
- `regulatory_fee`.
- `total_fees`.
- Complete persistent Reference Data Service model.
- Notification Service processing.
- `processed_events` downstream idempotency.
- Analytics consumer.
- Audit consumer.
- Production-scale Kafka partitioning.
- Performance benchmarking.
- Additional observability.

------------------------------------------------------------------------

# 20. Design Principles

## Loose Coupling

Kafka decouples producers, processors, and downstream consumers.

## Reliability

The Transactional Outbox ensures that the trade and its corresponding Outbox Event are persisted atomically.

## Fault Tolerance

Transient failures are retried and unrecoverable failures can be routed to a DLT.

## Idempotency

Duplicate Kafka deliveries do not create duplicate trades.

## Performance

Redis avoids repeated reference-data network calls.

## Extensibility

Fee calculation and additional downstream consumers can be added without changing the fundamental event-driven architecture.

------------------------------------------------------------------------

# 21. Documentation Maintenance Rule

This HLD is a living architecture document.

Whenever a major capability is implemented:

1.  Update the current architecture.
2.  Move the relevant capability from Planned to Implemented.
3.  Update database schemas.
4.  Update Kafka topics and flows if they changed.
5.  Update processing sequences.
6.  Preserve the architectural reasoning that led to the change.

This keeps the HLD aligned with the actual application while preserving the evolution of the design.
