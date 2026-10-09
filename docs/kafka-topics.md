# Kafka Topics — Trade Processing Pipeline

## Purpose

This document is the operational reference for Kafka topics in the Trade Processing Pipeline. It records the current topic and consumer configuration known from the project, and separates implemented behavior from the earlier design and future targets.

**Source-of-truth rule:** verify the actual topic list, partition count, retry-topic names, replication factor, and consumer-group offsets against the running Kafka environment and application configuration. A topic mentioned in an HLD diagram is not automatically created or used by the current application.

------------------------------------------------------------------------

# 1. Topic Inventory

| Topic                     | Role                                                             | Producer                         | Consumer                                 | Status                                                                           |
|---------------------------|------------------------------------------------------------------|----------------------------------|------------------------------------------|----------------------------------------------------------------------------------|
| `trade-events`            | Incoming trade events                                            | `trade-producer`                 | `trade-processor`                        | Implemented                                                                      |
| `trade-notifications`     | Downstream event destination configured on current Outbox Events | Outbox Publisher                 | Notification consumer intended           | Publishing implemented; full consumer flow planned                               |
| Spring Kafka retry topics | Retry failed input processing                                    | Spring Kafka retry mechanism     | Trade Processor retry listener           | Retry mechanism implemented; inspect runtime configuration for exact topic names |
| Dead Letter Topic (DLT)   | Receives records after retry handling is exhausted               | Spring Kafka retry/DLT mechanism | DLT handler / later operational recovery | DLT handling implemented; exact topic name must be verified                      |

The earlier design used `trade-processed`, `trade-retry-1`, `trade-retry-2`, `trade-retry-3`, and `trade-failed`. Treat those as historical design names unless they are confirmed in the current runtime configuration.

------------------------------------------------------------------------

# 2. `trade-events`

## Purpose

Carries newly submitted trade events from the Trade Producer to the Trade Processor.

## Ownership

- **Producer:** Trade Producer (`trade-producer`)
- **Consumer:** Trade Processor (`trade-processor`)
- **Consumer group:** `trade-processor-group`
- **Message key:** `tradeId`
- **Payload:** JSON representing a `TradeEvent`

Example payload:

``` json
{
  "tradeId": "TR123",
  "symbol": "AAPL",
  "quantity": 100,
  "price": 220,
  "side": "BUY"
}
```

The actual `TradeEvent` DTO in source is authoritative for field names and types.

## Why use a Kafka topic here?

Kafka separates the HTTP-facing producer from the processor. The producer can publish an event without synchronously waiting for the entire downstream workflow to complete. The processor can consume independently and recover from failures using Kafka’s delivery and retry mechanisms.

## Why use `tradeId` as the key?

The key helps route events for the same trade to the same partition under a stable partitioning configuration. Ordering is per partition; this does not establish a global order across all trades.

## Current partition configuration

The local project setup has been documented with **one partition** for `trade-events`. The earlier HLD proposed multiple partitions as a future scaling configuration. Verify the live topic with Kafka CLI before relying on the configured count.

------------------------------------------------------------------------

# 3. `trade-notifications`

## Purpose

The current Outbox Event configuration stores `trade-notifications` as the destination topic. The Outbox Publisher reads pending records from PostgreSQL and sends their stored payload to this topic.

## Current publisher behavior

1.  Query Outbox Events whose status is `PENDING`.
2.  Send the stored payload to the topic saved on the Outbox Event.
3.  Wait for the Kafka send result.
4.  If sending succeeds, set the Outbox status to `PUBLISHED` and record `publishedAt`.
5.  If sending fails, leave the event `PENDING` for a later scheduled attempt.

The Outbox Publisher currently runs on a 60-second initial delay and a 60-second fixed delay.

## Important distinction

Publishing to `trade-notifications` is implemented. The full Notification Service consumer, simulated email/SMS/webhook behavior, and `processed_events`-based downstream idempotency are planned and must not be described as fully implemented.

The older HLD used `trade-processed`. The current Outbox configuration uses `trade-notifications`.

## Current payload

The current consumer serializes a `TradeEvent` payload for storage in the Outbox Event. The publisher sends that stored payload. A dedicated downstream DTO such as `ProcessedTradeEvent` was proposed in the original design but is not yet the authoritative current payload contract.

------------------------------------------------------------------------

# 4. Retry Topics and Dead Letter Handling

## Current mechanism

The Trade Processor uses Spring Kafka’s `@RetryableTopic` and `@DltHandler`.

The current consumer configuration has been documented with four attempts and exponential backoff settings. Confirm the annotation and effective runtime configuration in source before changing retry assumptions.

## Retry concept

``` text
trade-events
     |
     v
Trade Processor
     |
     +---- processing succeeds ----> complete
     |
     +---- processing fails -------> retry handling
                                      |
                                      +---- later attempt succeeds
                                      |
                                      +---- retries exhausted
                                                   |
                                                   v
                                                  DLT
```

## What does the DLT do?

A Dead Letter Topic is a destination for records that could not be processed successfully within the configured retry policy. It keeps failed records available for investigation and possible controlled replay instead of silently discarding them.

## Topic-name caution

The original design described:

``` text
trade-retry-1
trade-retry-2
trade-retry-3
trade-failed
```

Spring Kafka can derive retry and DLT topic names from configuration. Do not assume these exact names exist. Inspect the running broker and application configuration.

------------------------------------------------------------------------

# 5. Consumer Group

Current consumer group:

``` text
trade-processor-group
```

A consumer group identifies the logical set of consumers that share work for a topic. Within a group, a partition is assigned to at most one consumer at a time. Multiple groups can independently consume the same topic for different purposes.

With one partition, a single consumer in the group can actively consume that partition at a time. Additional consumers in the same group do not increase parallelism beyond the available partitions.

------------------------------------------------------------------------

# 6. Partitions, Ordering, and Scaling

A topic is divided into partitions. Partitions allow parallel consumption and distribute data across brokers.

The original HLD proposed a future design with multiple partitions for the main input/output topics. The local development setup currently uses one partition for `trade-events`.

Important concepts:

- Ordering is guaranteed within a partition, not across all partitions.
- A message key helps related records map to the same partition.
- Increasing partitions can increase potential parallelism.
- Partition count should be chosen based on throughput, ordering needs, consumer count, and deployment capacity.
- Changing partition count can affect key-to-partition mapping; do not assume historical and new records for a key will always map identically after a partition-count change.

The HLD’s suggested partition counts are design targets, not benchmarked performance results.

------------------------------------------------------------------------

# 7. Message Delivery and Idempotency

## At-most-once

A record is delivered zero or one time from the application’s perspective. Data loss can occur if a failure happens after progress is recorded but before processing completes.

## At-least-once

A record may be delivered more than once, but retry/recovery behavior is designed to avoid losing it. Duplicate processing must be handled.

The current Outbox Publisher follows an at-least-once approach: a crash after Kafka acknowledges a send but before the database row is marked `PUBLISHED` may cause another send.

## Exactly-once

Exactly-once claims require precise scope and configuration. Kafka’s transactional features do not automatically make a workflow involving PostgreSQL and external side effects exactly once.

## Current idempotency

The Trade Processor checks whether `tradeId` already exists and the database enforces uniqueness on `trade_id`. This protects trade persistence, but it does not automatically prevent duplicate downstream notifications.

Downstream idempotency using a stable event ID and `processed_events` is planned.

------------------------------------------------------------------------

# 8. Outbox and Kafka Consistency

PostgreSQL and Kafka are separate systems and are not committed by the application’s local database transaction together.

The Transactional Outbox solves the database-side dual-write problem:

``` text
One PostgreSQL transaction
    |
    +---- save trade
    |
    +---- save outbox_event with PENDING status
    |
    v
Commit both together
```

A separate publisher sends the Outbox payload to Kafka. If Kafka is unavailable, the Outbox row remains `PENDING` and can be retried later.

The publisher waits for the Kafka send result before marking the row `PUBLISHED`. This avoids marking a failed send as published, while accepting the possibility of duplicate publication in the crash window.

------------------------------------------------------------------------

# 9. Useful Local Kafka Commands

Use the commands documented in `commands.md` as the project’s primary command reference. Typical Kafka CLI operations include:

## List topics

``` bash
kafka-topics.sh --bootstrap-server localhost:9092 --list
```

## Describe a topic

``` bash
kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic trade-events
```

## Describe the downstream topic

``` bash
kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic trade-notifications
```

## Inspect consumer groups

``` bash
kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list
```

## Describe the Trade Processor consumer group

``` bash
kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group trade-processor-group
```

These examples assume the CLI is available in the shell where the command runs. In this project, Kafka runs in Docker, so execute the commands from the appropriate container or use the exact `docker exec` wrappers already documented in `commands.md`.

------------------------------------------------------------------------

# 10. Verification Checklist

Before describing Kafka configuration as current, verify:

- [ ] `trade-events` exists.
- [ ] `trade-events` partition count matches the intended local configuration.
- [ ] `trade-processor-group` is active when the processor is running.
- [ ] `trade-notifications` exists when the publisher first attempts to use it, or topic auto-creation/configuration handles it.
- [ ] The current retry and DLT topic names match the Spring Kafka configuration.
- [ ] Message keys are set as expected.
- [ ] The payload matches the current DTO contract.
- [ ] Consumer lag and retry behavior are observable in the local environment.

------------------------------------------------------------------------

# 11. Current vs Planned Summary

| Capability                                        | Status                          |
|---------------------------------------------------|---------------------------------|
| Trade Producer publishes to `trade-events`        | Implemented                     |
| Trade Processor consumes `trade-events`           | Implemented                     |
| Consumer group `trade-processor-group`            | Configured                      |
| Trade-level duplicate check                       | Implemented                     |
| Spring Kafka retry/DLT handling                   | Implemented                     |
| Outbox Publisher sends to `trade-notifications`   | Implemented                     |
| Outbox `PENDING` → `PUBLISHED` lifecycle          | Implemented                     |
| Notification Service consumes downstream events   | Planned / not fully implemented |
| `processed_events` downstream idempotency         | Planned                         |
| Analytics and Audit consumers                     | Planned                         |
| Production partition tuning and load benchmarking | Planned                         |

------------------------------------------------------------------------

# 12. Related Documentation

- `HLD.md` — architecture and design evolution.
- `LLD.md` — consumer, service, and publisher responsibilities.
- `api-contracts.md` — event payloads and HTTP contracts.
- `sequence-diagrams.md` — runtime interactions and failure scenarios.
- `database.md` — Outbox persistence and related schemas.
- `commands.md` — local Kafka and Docker commands.
