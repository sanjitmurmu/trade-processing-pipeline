# High Level Design

## 1. Overview

The Trade Processing Pipeline is an event-driven system designed to receive trade events, process and enrich them, persist the resulting trade, and publish downstream events reliably.

The architecture evolved incrementally as new reliability and performance requirements were introduced:

``` text
Initial Architecture
        ↓
Redis Caching
        ↓
Retry + DLQ
        ↓
Transactional Outbox
        ↓
Idempotent Processing
        ↓
Database / Event Design
```

This document preserves that architectural evolution while also identifying the current implementation and planned work.

------------------------------------------------------------------------

## 2. Major Components

The original architecture consists of the following components:

1.  Trade Producer
2.  Kafka
3.  Trade Processor
4.  Reference Data Service
5.  PostgreSQL
6.  Notification Service

### 2.1 Trade Producer

The Trade Producer simulates an upstream trading system.

It accepts trade requests and publishes trade events to Kafka.

**Input topic:**

``` text
trade-events
```

### 2.2 Kafka

Kafka provides the event backbone between the producer, processor, and downstream consumers.

The input trade event is published using `tradeId` as the Kafka message key so that events for the same trade can maintain ordering within a partition.

### 2.3 Trade Processor

The Trade Processor contains the main processing logic.

At a high level it:

- Consumes trade events from Kafka.
- Performs duplicate detection.
- Obtains reference data.
- Enriches the trade.
- Calculates fees and taxes — **planned; not yet implemented**.
- Persists the trade.
- Persists an Outbox Event in the same database transaction.
- Relies on the Outbox Publisher to publish the downstream event.

### 2.4 Reference Data Service

The Reference Data Service provides reference information for a symbol.

Example endpoint:

``` text
GET /reference/AAPL
```

The implemented response includes the symbol:

``` json
{
  "symbol": "AAPL",
  "exchange": "NASDAQ",
  "currency": "USD",
  "sector": "TECH"
}
```

> This response differs from the older HLD version, which did not include `symbol`. The current implementation is the authoritative contract.

### 2.5 PostgreSQL

PostgreSQL stores:

- Processed trades.
- Outbox events.
- Reference data where applicable to the supporting service.

The Trade Processor uses PostgreSQL as the transactional boundary for trade persistence and Outbox Event persistence.

### 2.6 Redis

Redis is used as a cache for reference data.

The cache reduces repeated calls from the Trade Processor to the Reference Data Service when the same symbol is processed repeatedly.

### 2.7 Notification Service

The original architecture includes a Notification Service that consumes downstream trade events and can simulate:

- Email notifications.
- SMS notifications.
- Webhooks.

The service is part of the target multi-service architecture; its full downstream processing implementation is still planned.

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

The Trade Processor consumed the event, enriched it with reference data, and persisted the processed trade.

The initial design exposed an important performance problem.

------------------------------------------------------------------------

# 4. HLD V1 — Redis Caching

## 4.1 Problem

The Trade Processor needs reference data such as:

``` json
{
  "symbol": "AAPL",
  "exchange": "NASDAQ",
  "currency": "USD",
  "sector": "TECH"
}
```

If every trade causes a REST request to the Reference Data Service:

``` text
1 million trades
       ↓
1 million REST calls
```

This creates unnecessary network traffic and additional latency.

## 4.2 Redis Solution

Redis was introduced as a cache:

``` text
Trade Processor
      |
      v
    Redis
      |
   HIT? ───── YES ───> Return cached reference data
      |
      NO
      |
      v
Reference Data Service
      |
      v
Store in Redis
      |
      v
Return reference data
```

Example:

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

This allows subsequent trades for the same symbol to avoid repeated REST calls.

> The original HLD used example latency figures such as 20 ms versus 1 ms. These are illustrative design examples, not benchmark results for the current implementation.

------------------------------------------------------------------------

# 5. HLD V2 — Retry and Dead Letter Handling

## 5.1 Why Retry?

Reference Data Service failures may be temporary.

For example:

``` text
Trade Processor
      |
      v
Reference Data Service
      X
   temporary failure
```

Immediately discarding the trade could result in data loss.

The system therefore uses retry handling for failures that may recover.

## 5.2 Exponential Backoff

The original design illustrates:

``` text
Attempt 1 → failure
    ↓
  wait 1s
    ↓
Attempt 2 → failure
    ↓
  wait 2s
    ↓
Attempt 3 → failure
    ↓
  wait 4s
    ↓
Attempt 4 → failure
    ↓
  DLQ
```

The purpose of exponential backoff is to avoid continuously hammering a failing dependency.

## 5.3 Permanent Failures

Some failures are not transient.

Example:

``` json
{
  "tradeId": "TR456",
  "quantity": -10
}
```

Retrying an invalid trade repeatedly will not make it valid.

Such messages should ultimately be routed to a Dead Letter mechanism for inspection and possible replay.

Conceptually:

``` text
trade-events
     |
     v
Trade Processor
     |
     v
Processing Failure
     |
     v
Dead Letter Topic
```

## 5.4 Current Implementation

The current Trade Processor uses Spring Kafka retry handling with `@RetryableTopic` and a `@DltHandler`.

Therefore, the current implementation should be understood as:

``` text
Kafka trade event
       |
       v
Trade Processor
       |
   processing
       |
   failure?
       |
       v
Spring Kafka retry handling
       |
       v
D LT / @DltHandler
```

The older HLD’s manually named `trade-retry-1`, `trade-retry-2`, and `trade-retry-3` topics describe the architectural concept, not the exact current configuration.

------------------------------------------------------------------------

# 6. HLD V3 — Transactional Outbox

## 6.1 The Consistency Problem

Suppose the application performs:

``` text
Save Trade
    ↓
Publish Kafka Event
```

Two operations are involved:

- PostgreSQL
- Kafka

If PostgreSQL succeeds but Kafka publishing fails:

``` text
PostgreSQL → SUCCESS
Kafka      → FAILURE
```

The trade exists in the database, but downstream consumers never receive the event.

The opposite ordering has the reverse problem:

``` text
Kafka      → SUCCESS
PostgreSQL → FAILURE
```

Now downstream systems believe the trade exists even though the database does not contain it.

A distributed transaction across Kafka and PostgreSQL would introduce significant complexity and is not the approach selected for this project.

## 6.2 Transactional Outbox Solution

The selected design is the Transactional Outbox Pattern.

The Trade Processor performs both database writes inside one database transaction:

``` text
              PostgreSQL Transaction
              ┌─────────────────────┐
              │ Save Trade           │
              │         AND          │
              │ Save Outbox Event    │
              └─────────────────────┘
                        |
                     COMMIT
                        |
              Both succeed together
```

If the transaction fails:

``` text
Trade save       → rollback
Outbox save      → rollback
```

This guarantees that a successfully persisted trade has a corresponding Outbox Event.

## 6.3 Outbox Publisher

Kafka publishing is deliberately separated from the transaction that stores the trade.

The Outbox Publisher periodically finds pending events:

``` text
PostgreSQL
    |
    v
Outbox Event
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
Retry on next scheduler run
```

When Kafka publishing succeeds:

``` text
PENDING
   |
   v
Kafka publish successful
   |
   v
PUBLISHED
```

## 6.4 Current Outbox Implementation

The current implementation uses:

``` text
Table:
outbox_event

Statuses:
PENDING
PUBLISHED
```

The current downstream topic configured in the Outbox Event is:

``` text
trade-notifications
```

The publisher waits for the Kafka send result before marking the event `PUBLISHED`.

This provides an **at-least-once publishing model**. A crash after Kafka accepts the event but before the database status is updated can result in the same event being published again. The design therefore relies on downstream idempotency where required.

------------------------------------------------------------------------

# 7. HLD V4 — Idempotent Processing

## 7.1 Why Duplicates Can Happen

Kafka commonly provides at-least-once processing semantics.

Consider:

``` text
Consumer receives TR123
       |
       v
Trade persisted
       |
       X
Consumer crashes before offset commit
       |
       v
Kafka redelivers TR123
```

Without idempotency, the trade could be processed twice.

## 7.2 Trade ID as Idempotency Key

`tradeId` is used as the natural idempotency key.

Before processing a trade:

``` text
Does tradeId already exist?
       |
   ┌───┴───┐
  YES      NO
   |        |
 Skip     Process
```

The database also enforces uniqueness on `trade_id`.

This gives us two layers of protection:

1.  Application-level duplicate detection.
2.  Database-level unique constraint.

## 7.3 Current Implementation

The current Trade Processor checks the repository before processing:

``` text
findByTradeId(tradeId)
```

If the trade already exists:

``` text
Duplicate trade received
        ↓
Skip processing
```

The `trade_id` column is also unique in PostgreSQL.

------------------------------------------------------------------------

# 8. HLD V5 — Database and Event Design

## 8.1 Trade Table

The current Trade table contains the core processed trade information:

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

The current implementation does **not yet contain `regulatory_fee` or `total_fees`** because fee calculation is the next development milestone.

## 8.2 Outbox Table

The current implementation uses:

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

The payload is stored as text in the current implementation.

## 8.3 Future Fee Fields

Once fee calculation is implemented, the Trade table is expected to contain:

``` text
brokerage_fee
tax
regulatory_fee
total_fees
```

The conceptual calculation will be:

``` text
tradeValue       = price × quantity

brokerageFee     = tradeValue × brokerageRate
tax              = tradeValue × taxRate
regulatoryFee    = tradeValue × regulatoryRate

totalFees        = brokerageFee
                 + tax
                 + regulatoryFee
```

The exact rate configuration and database model will be designed before implementation.

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

The event key is the `tradeId`.

## 9.2 Downstream Topic

The current Outbox configuration publishes to:

``` text
trade-notifications
```

The historical HLD used the name:

``` text
trade-processed
```

The latter represents the earlier architectural design; the current implementation uses `trade-notifications`.

## 9.3 Retry and DLQ

The original HLD described:

``` text
trade-retry-1
trade-retry-2
trade-retry-3
trade-failed
```

These represent the retry/DLQ architecture concept.

The current implementation delegates retry-topic handling to Spring Kafka’s `@RetryableTopic` mechanism rather than manually implementing the retry-topic flow described above.

------------------------------------------------------------------------

# 10. Event Ordering

The Kafka message key is:

``` text
tradeId
```

Therefore, events for the same trade are routed to the same partition.

Conceptually:

``` text
TR123 event 1 ─┐
TR123 event 2 ─┼──> Same Kafka partition
TR123 event 3 ─┘
```

This allows ordering to be maintained for events belonging to the same trade.

The original HLD proposed multiple partitions for horizontal processing. The current local setup uses a single partition for `trade-events`; partition count can be increased as the system is scaled.

------------------------------------------------------------------------

# 11. Failure Scenarios

## 11.1 Reference Data Service Temporarily Unavailable

``` text
Trade Processor
      |
      X
Reference Data Service
      |
      v
Retry
      |
      +---- success → Continue
      |
      +---- repeated failure → DLT
```

## 11.2 Invalid Trade

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

The invalid event is retained for investigation rather than silently discarded.

## 11.3 Kafka Unavailable During Outbox Publishing

``` text
Trade + Outbox Event
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
```

This prevents the database transaction from depending on Kafka availability.

## 11.4 Duplicate Kafka Delivery

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
 Skip
```

------------------------------------------------------------------------

# 12. Current End-to-End Architecture

The current implemented flow is:

``` text
Client
  |
  v
Trade Producer
  |
  |  trade event
  v
Kafka: trade-events
  |
  v
Trade Processor
  |
  +----> Redis
  |         |
  |         +---- HIT → reference data
  |         |
  |         +---- MISS → Reference Data Service
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

Fee calculation is the next major processing capability to be added between enrichment and persistence.

------------------------------------------------------------------------

# 13. Target Processing Flow

After the fee-calculation milestone, the intended processing flow will become:

``` text
Trade Event
    |
    v
Duplicate Check
    |
    v
Reference Data Enrichment
    |
    v
Fee Calculation
    |
    +--> Trade Value
    |
    +--> Brokerage Fee
    |
    +--> Tax
    |
    +--> Regulatory Fee
    |
    +--> Total Fees
    |
    v
Trade + Outbox Event
    |
    v
Single DB Transaction
    |
    v
PostgreSQL
    |
    v
Outbox Publisher
    |
    v
Kafka
```

------------------------------------------------------------------------

# 14. Architectural Evolution Summary

| Version | Problem                        | Solution                                     |
|---------|--------------------------------|----------------------------------------------|
| Initial | Basic trade processing         | Kafka + Trade Processor + PostgreSQL         |
| V1      | Excessive reference-data calls | Redis cache                                  |
| V2      | Temporary/permanent failures   | Retry + DLQ                                  |
| V3      | DB/Kafka consistency           | Transactional Outbox                         |
| V4      | Duplicate processing           | Idempotency using trade ID                   |
| V5      | Clear persistence/event model  | Trade + Outbox + supporting schemas          |
| Next    | Fees not yet implemented       | Fee Calculation Service + configurable rates |

------------------------------------------------------------------------

# 15. Current vs Planned

## Implemented

- Trade ingestion through Kafka.
- Reference Data Service integration.
- Redis reference-data caching.
- Trade persistence.
- Transactional Outbox.
- Outbox Publisher.
- Retry handling with Spring Kafka.
- DLT handling.
- Duplicate trade detection.
- Database uniqueness for `trade_id`.

## Planned

- Fee Calculation Service.
- Configurable fee rates.
- Regulatory fee persistence.
- Total fee persistence.
- Full Notification Service processing.
- Additional downstream consumers.
- Production-scale partitioning and performance benchmarking.
- Additional observability and automated testing.

------------------------------------------------------------------------

# 16. Design Principles

The architecture follows several core principles:

### Loose Coupling

Kafka decouples producers, processors, and downstream consumers.

### Reliability

The Transactional Outbox prevents a successfully persisted trade from being left without its corresponding downstream event.

### Fault Tolerance

Transient failures are retried and unrecoverable failures can be routed to a DLT.

### Idempotency

Duplicate Kafka deliveries do not create duplicate trades.

### Performance

Redis avoids repeated reference-data network calls.

### Extensibility

Fee calculation and additional downstream consumers can be added without changing the fundamental event-driven architecture.
