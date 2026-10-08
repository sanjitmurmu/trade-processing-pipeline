# Trade Processing Pipeline

## 1. Problem Statement

Build an event-driven trade processing system that:

- Receives trade events from upstream systems.
- Validates trades.
- Enriches trades with reference data.
- Calculates fees and taxes.
- Stores processed trades.
- Publishes downstream events.
- Handles failures gracefully.

The project is inspired by real-world trading systems used by financial
institutions.

------------------------------------------------------------------------

## 2. Functional Requirements

### FR1: Trade Ingestion

The system must accept trade events from upstream systems.

Example:

``` json
{
  "tradeId": "TR123",
  "symbol": "AAPL",
  "quantity": 100,
  "price": 220,
  "side": "BUY"
}
```

### FR2: Validation

The system must validate:

- Trade ID uniqueness.
- Quantity \> 0.
- Price \> 0.
- Valid symbol.

Invalid trades should be routed to a Dead Letter Queue (DLQ).

### FR3: Reference Data Enrichment

The system must enrich trades with:

- Exchange.
- Currency.
- Sector.

### FR4: Fee Calculation

The system must calculate:

- Brokerage fee.
- Tax.
- Regulatory fee.

The requirements document does not prescribe the exact fee formulas or
rates. The fee calculation model will be designed and implemented as a
separate development milestone.

### FR5: Persistence

The system must store successfully processed trades.

### FR6: Event Publication

The system must publish processed trade events to Kafka for downstream
consumers.

------------------------------------------------------------------------

## 3. Non-Functional Requirements

| Requirement     | Target                |
|-----------------|-----------------------|
| Throughput      | 1000 TPS              |
| Availability    | 99.9%                 |
| Scalability     | Horizontal            |
| Fault Tolerance | Retry + DLQ           |
| Event Ordering  | Partition by trade ID |

### Throughput

The target throughput is **1000 transactions per second (TPS)**.

This is a system design target; it is not a claim that the current
implementation has already been benchmarked at 1000 TPS.

### Availability

The target availability is **99.9%**.

### Scalability

The system should support horizontal scaling of processing components.

### Fault Tolerance

The system should handle failures using retry mechanisms and Dead Letter
Queues.

### Event Ordering

Events should be partitioned by `tradeId` so that events belonging to
the same trade can maintain ordering within a Kafka partition.

------------------------------------------------------------------------

## 4. Requirement Scope

The requirements describe **what the system should do**, rather than how
the system should implement it.

Implementation details such as:

- Kafka configuration.
- Redis caching.
- Transactional Outbox.
- Database schema.
- Retry configuration.
- Consumer idempotency.
- Fee configuration storage.

belong in the High-Level Design (HLD), Low-Level Design (LLD), and
implementation documentation.

------------------------------------------------------------------------

## 5. Fee Calculation — Design Boundary

Fee calculation is a required capability, but the original requirements
do not define the exact formulas or rates.

The implementation will therefore establish these separately.

The intended calculation model will be based on the trade value:

``` text
tradeValue = price × quantity
```

and configurable rates can then be applied to calculate individual fees.

For example, conceptually:

``` text
brokerageFee   = tradeValue × brokerageRate
tax            = tradeValue × taxRate
regulatoryFee  = tradeValue × regulatoryRate
```

The exact rates, configuration model, database design, rounding rules,
and implementation will be decided during the fee-calculation
development milestone.

------------------------------------------------------------------------

## 6. Requirement Status

| Requirement                  | Status                                       |
|------------------------------|----------------------------------------------|
| Trade ingestion              | Implemented                                  |
| Trade validation             | Partially implemented / further work planned |
| Reference data enrichment    | Implemented                                  |
| Fee calculation              | Planned                                      |
| Trade persistence            | Implemented                                  |
| Downstream event publication | Implemented through the Outbox pattern       |
| Retry and failure handling   | Implemented                                  |
| Idempotent trade processing  | Implemented                                  |
| Redis caching                | Implemented                                  |
| 1000 TPS target              | Not benchmarked                              |
| 99.9% availability target    | Design target                                |

> **Note:** The status table reflects the current project implementation
> and is separate from the original functional requirements. It is
> included to keep the documentation synchronized as development
> progresses.
