# Trade Processing Pipeline — Backend Interview Q&A

## Purpose

This is a learning guide for understanding and explaining the Trade Processing Pipeline in backend engineering interviews.

For each topic, aim to explain:

- **What:** definition.
- **Why:** the problem it solves and why this project uses it.
- **How:** how it works in this application.
- **Trade-offs:** limitations and alternatives.

**Accuracy rule:** the project has implemented and planned capabilities. Interview answers must distinguish them. Do not claim fee calculation, full Notification Service processing, downstream `processed_events` idempotency, production-scale partitioning, or performance benchmarks are complete until they are implemented and verified.

------------------------------------------------------------------------

# Part 1 — Project Overview

## Q1. What is the Trade Processing Pipeline?

**Answer:** It is an event-driven backend application that accepts trade requests, publishes trade events to Kafka, consumes them in a Trade Processor, enriches them with reference data, persists the trade in PostgreSQL, and records a downstream event in an Outbox table. A separate publisher sends pending Outbox events to Kafka.

The project is designed to demonstrate backend concepts used in financial systems: asynchronous processing, persistence, reliability, idempotency, caching, retry handling, and service boundaries.

## Q2. What problem does the application solve?

**Answer:** It separates trade ingestion from trade processing and downstream publication. The HTTP-facing producer does not need to perform the entire workflow synchronously. Kafka decouples services, PostgreSQL stores the business data, Redis reduces repeated reference-data lookups, and the Outbox pattern helps avoid losing downstream events when Kafka is temporarily unavailable.

## Q3. Walk me through the current end-to-end flow.

**Answer:**

1.  A client submits a trade to the Trade Producer.
2.  The producer publishes a `TradeEvent` to Kafka topic `trade-events`.
3.  `TradeConsumerService` consumes the event.
4.  The consumer checks whether the `tradeId` already exists.
5.  It retrieves reference data through the Redis/reference-service integration.
6.  It creates a `TradeEntity` and an `OutboxEvent`.
7.  `TradePersistenceService` saves both in one PostgreSQL transaction.
8.  `OutboxPublisherService` periodically reads Outbox events with status `PENDING`.
9.  It sends the stored payload to `trade-notifications`.
10. After Kafka acknowledges the send, the publisher marks the Outbox row `PUBLISHED`.

Fee calculation and the full Notification Service consumer are planned additions, not part of the completed end-to-end flow.

## Q4. Why is this a good backend portfolio project?

**Answer:** It demonstrates more than CRUD. It brings together API design, Kafka messaging, asynchronous processing, persistence, transactional consistency, retries, dead-letter handling, duplicate protection, caching, Docker-based local infrastructure, and architectural documentation. The value comes from understanding the failure cases and trade-offs, not simply listing technologies.

## Q5. What are the current modules?

**Answer:**

- `trade-producer`: accepts trade requests and publishes input events.
- `trade-processor`: consumes events, enriches and persists trades, and creates Outbox events.
- `reference-data-service`: provides reference data.
- `trade-common`: intended for shared DTOs/enums/constants where needed.
- `notification-service`: planned downstream processing; not yet fully implemented.

The current implementation and exact module contents should be checked against the repository before describing every class as present.

------------------------------------------------------------------------

# Part 2 — HTTP APIs and DTOs

## Q6. What is an API contract?

**Answer:** An API contract defines how two components communicate: endpoint or topic, request/payload fields, types, required values, response shape, and error behavior. A stable contract lets services evolve without making incompatible assumptions about one another.

**In this project:** `api-contracts.md` documents the Trade Producer request, the Reference Data API response, and the Kafka payloads. Exact HTTP response codes and error formats should be confirmed against controller code before being quoted as facts.

## Q7. What is a DTO?

**Answer:** A Data Transfer Object carries data across a boundary, such as an HTTP request or Kafka message. It separates the external message shape from internal persistence entities.

**Why use it?** It avoids exposing database entities directly and gives the application a defined serialization contract.

## Q8. What is `TradeEvent`?

**Answer:** `TradeEvent` represents the incoming trade message sent to `trade-events`. Its documented example includes `tradeId`, `symbol`, `quantity`, `price`, and `side`. The actual shared DTO in source is authoritative for exact types and fields.

## Q9. Why does the Reference Data response include `symbol`?

**Answer:** The response contract was updated during development to include `symbol` alongside `exchange`, `currency`, and `sector`. Including the symbol makes the response self-describing and keeps the API response aligned with the requested instrument. The field must remain documented in the API contract and LLD.

## Q10. Why should we not assume HTTP success means the trade is fully processed?

**Answer:** The system is asynchronous. A successful submission response may mean the producer accepted the request or published the input event; it does not necessarily mean the Trade Processor has persisted the trade. The exact meaning depends on the controller implementation and must be documented accurately.

------------------------------------------------------------------------

# Part 3 — Kafka Fundamentals

## Q11. What is Apache Kafka?

**Answer:** Kafka is a distributed event-streaming platform. Producers write records to topics, and consumers read them independently. Records are stored in partitions and can be retained for configured periods.

**Why use it here?** It decouples the Trade Producer from the Trade Processor, supports asynchronous processing, and provides a durable stream that can be consumed independently of the HTTP request.

## Q12. What is a Kafka topic?

**Answer:** A topic is a named stream of records. In this project, `trade-events` carries input trade events, while `trade-notifications` is the current downstream topic configured on Outbox events.

## Q13. What is a producer?

**Answer:** A producer writes records to Kafka. The Trade Producer sends input events to `trade-events`; the Outbox Publisher sends stored Outbox payloads to the topic specified on each Outbox Event.

## Q14. What is a consumer?

**Answer:** A consumer reads records from Kafka. `TradeConsumerService` handles input trade events. A complete Notification Service consumer is planned.

## Q15. What is a consumer group?

**Answer:** A consumer group is a set of consumers cooperating to process a topic. Kafka assigns each partition to one consumer within a group at a time. Separate groups can read the same topic independently.

**In this project:** the input consumer group is `trade-processor-group`.

## Q16. What is a partition?

**Answer:** A partition is an ordered, append-only sequence of records within a topic. Multiple partitions allow parallel processing across consumers.

**Current project:** the local `trade-events` setup has been documented with one partition. Multi-partition configuration is a scaling target, not proof of measured throughput.

## Q17. Why use `tradeId` as the Kafka key?

**Answer:** The key helps Kafka route events for the same trade to the same partition under a stable partitioning configuration. This supports ordering for that key within the partition. It does not provide global ordering across all trades.

## Q18. What happens if there is one partition and four consumers in a group?

**Answer:** Only one consumer in that group can actively own that partition at a time. The other consumers will be idle for that topic until more partitions are available or they consume other topics.

## Q19. What is a Kafka offset?

**Answer:** An offset identifies a record’s position within a partition. A consumer tracks progress through offsets so it can resume after restart. Offset management affects whether records may be redelivered after failures.

## Q20. What is consumer lag?

**Answer:** Consumer lag is the difference between the latest available offset and the consumer group’s committed/current position. High lag can indicate that messages arrive faster than they are processed, consumers are unhealthy, or downstream dependencies are slow.

## Q21. Does Kafka guarantee ordering?

**Answer:** Kafka guarantees order within a partition. It does not provide a total ordering across all partitions. Using `tradeId` as a key helps related events share a partition, but changing partition count can change key-to-partition mapping.

## Q22. What is the difference between a topic and a queue?

**Answer:** A traditional queue often distributes each message to one competing consumer. Kafka stores an ordered log that can be read by consumer groups independently, with replay based on offsets and retention. Kafka is not simply a one-time message handoff.

------------------------------------------------------------------------

# Part 4 — Delivery Semantics, Retry, and DLT

## Q23. What is at-most-once delivery?

**Answer:** A record is processed zero or one time from the application’s perspective. A failure can cause loss if progress is recorded before processing is safely completed.

## Q24. What is at-least-once delivery?

**Answer:** The system retries or redelivers records so that a record is not silently lost under the intended failure model. A record can be delivered more than once, so processing must tolerate duplicates.

**In this project:** the Outbox Publisher uses an at-least-once approach. If Kafka accepts a send but the application crashes before the database status changes to `PUBLISHED`, the same event may be sent again.

## Q25. What does exactly-once mean?

**Answer:** Exactly-once claims must define the scope. Kafka supports transactional and idempotent producer features, but they do not automatically make a workflow involving PostgreSQL and external side effects exactly once. Cross-system effects need their own consistency and idempotency design.

## Q26. What is a retry?

**Answer:** A retry is another attempt after an operation fails. It is useful for transient failures such as a temporarily unavailable service or network issue. It is not useful for every error; invalid business data may fail repeatedly until the data changes.

## Q27. What is exponential backoff?

**Answer:** Exponential backoff increases the delay between retry attempts, for example 1 second, then 2 seconds, then 4 seconds. It reduces pressure on a failing dependency and gives it time to recover.

The actual attempt count and delays must match the current Spring Kafka configuration.

## Q28. What is a Dead Letter Topic (DLT)?

**Answer:** A DLT stores records that failed processing after the configured retry policy is exhausted. It supports investigation and controlled replay instead of silently discarding failed records.

## Q29. What is `@RetryableTopic`?

**Answer:** It is a Spring Kafka mechanism that configures retry-topic handling for a listener. It manages retry delivery according to the configured policy. The exact generated retry topic names should be verified from the runtime configuration.

## Q30. What is `@DltHandler`?

**Answer:** It identifies a handler for records routed to the Dead Letter Topic by the retry-topic mechanism. In this project, the DLT handler logs failed-record metadata. A production system may also need alerting, ownership, replay procedures, and retention policies.

## Q31. Should every error be retried?

**Answer:** No. Transient infrastructure errors may recover. Invalid fields or unsupported business values generally require correction rather than repeated attempts. Retry classification should distinguish transient failures from permanent failures so retries do not waste resources.

------------------------------------------------------------------------

# Part 5 — Transactional Outbox

## Q32. What is the dual-write problem?

**Answer:** A dual-write problem occurs when an application must update a database and publish an event to another system. If the database write succeeds but event publication fails, the database and downstream systems disagree. If the event is published first and the database write fails, downstream systems may receive an event for data that was never committed.

## Q33. What is the Transactional Outbox Pattern?

**Answer:** The application writes the business record and an event record into the same database transaction. A separate publisher later reads the event record and sends it to the message broker.

**In this project:** `TradePersistenceService` saves the `TradeEntity` and `OutboxEvent` in one `@Transactional` method. `OutboxPublisherService` publishes pending events separately.

## Q34. Why save the Outbox Event in the same transaction as the trade?

**Answer:** It ensures that either both records are committed or both are rolled back. We avoid committing a trade without recording the event that must eventually be published.

## Q35. Why not publish to Kafka inside the database transaction?

**Answer:** A local PostgreSQL transaction cannot atomically commit a Kafka send. Publishing within the method does not make Kafka and PostgreSQL one transaction. The Outbox pattern records the intent to publish atomically in PostgreSQL, then retries publication separately.

## Q36. What does `PENDING` mean?

**Answer:** The Outbox Event has been saved but has not yet been marked successfully published by the publisher.

## Q37. What does `PUBLISHED` mean?

**Answer:** The publisher received a successful result from the Kafka send and updated the Outbox row to `PUBLISHED`, recording `publishedAt`. It does not mean the downstream consumer has completed its business processing.

## Q38. Why does the publisher wait on the Kafka send future with `.get()`?

**Answer:** `KafkaTemplate.send()` is asynchronous and returns a future. Waiting for its result lets the publisher distinguish a successful send from a failed send before changing the Outbox status. Without waiting for the result, it could mark an event published before knowing whether the send succeeded.

The acknowledgement reflects the configured Kafka producer/broker acknowledgement behavior; it is not a promise that a downstream consumer has processed the event.

## Q39. What if Kafka is down while the publisher runs?

**Answer:** The send fails, the exception is caught, and the Outbox row remains `PENDING`. A later scheduled run queries pending events and retries. This failure-and-recovery scenario has been tested in the project.

## Q40. What if the publisher crashes after Kafka accepts the event but before updating PostgreSQL?

**Answer:** The database row remains `PENDING`. After restart, the publisher can send the event again. This creates a duplicate but avoids incorrectly treating a potentially unpublished event as complete. This is the at-least-once trade-off.

## Q41. Does the Outbox guarantee exactly-once delivery?

**Answer:** No. It reliably records the need to publish and supports retries, but the crash window can cause duplicate sends. Downstream consumers should be idempotent when duplicate side effects matter.

## Q42. Why have a separate `TradePersistenceService`?

**Answer:** It isolates the transaction boundary. The consumer coordinates processing, while `TradePersistenceService` owns the atomic write of the trade and Outbox Event. This makes the consistency guarantee explicit and easier to test.

## Q43. Why does the Outbox Publisher use a scheduler?

**Answer:** The scheduled publisher periodically looks for `PENDING` records and attempts publication. The current configuration uses a 60-second initial delay and a 60-second fixed delay. A more advanced design could use batching, locking/claiming, or CDC depending on scale and operational needs.

## Q44. What if two publisher instances select the same pending event?

**Answer:** The current design should not be assumed to have distributed claim/locking semantics unless implemented in code. If multiple instances publish concurrently, they could send the same event. At higher scale, use an explicit claiming strategy, database locking, or another coordinated publisher design, and retain downstream idempotency.

------------------------------------------------------------------------

# Part 6 — PostgreSQL, JPA, and Transactions

## Q45. Why use PostgreSQL?

**Answer:** PostgreSQL provides relational persistence, transactions, constraints, indexes, and reliable querying. Trades and Outbox Events need durable storage and atomic updates, which fit a relational database well.

## Q46. What is JPA?

**Answer:** Java Persistence API is a specification for mapping Java objects to relational data. In Spring applications, an ORM implementation such as Hibernate commonly provides the runtime behavior.

## Q47. What is an entity?

**Answer:** An entity is a Java class mapped to a database table. `TradeEntity` maps trade fields; `OutboxEvent` maps the Outbox row.

## Q48. What is a repository?

**Answer:** A repository abstracts data access. In this project, `TradeRepository` handles trade persistence and lookup, while `OutboxEventRepository` handles Outbox persistence and status-based lookup.

Repositories should not become the place for workflow orchestration or business rules.

## Q49. What does `@Transactional` do?

**Answer:** It defines a transaction boundary managed by Spring. If the method completes successfully, the transaction can commit; if a qualifying exception causes rollback, the database writes are rolled back together. Exact rollback behavior depends on exception type and transaction configuration.

## Q50. Why is a unique constraint needed if we already check `tradeId` in Java?

**Answer:** The lookup and insert are separate operations. Two concurrent requests can both observe that a trade does not exist before either inserts. A database unique constraint is the final integrity guard against duplicate rows. The application should deliberately handle constraint violations if concurrency can cause them.

## Q51. What is the purpose of `@PrePersist` and `@PreUpdate`?

**Answer:** They are JPA lifecycle callbacks. In this project, `@PrePersist` sets creation timestamps and `@PreUpdate` refreshes the trade’s update timestamp. The Outbox Event also uses `@PrePersist` to set `createdAt`.

## Q52. Why use `BigDecimal` for financial values?

**Answer:** `BigDecimal` represents decimal values with explicit precision and scale, avoiding the binary floating-point approximation associated with `float` and `double`. It is generally more appropriate for prices and monetary calculations.

## Q53. What does precision 19 and scale 4 mean?

**Answer:** For `NUMERIC(19,4)`, the number can have up to 19 total decimal digits, of which 4 are after the decimal point. That leaves up to 15 digits before the decimal point. The appropriate scale depends on the financial rules of the application.

## Q54. Why does `trade_id` need a unique constraint?

**Answer:** `trade_id` is the business identifier used for duplicate detection. A unique constraint ensures that two committed rows cannot have the same trade identifier.

## Q55. Why is the Outbox payload stored as text?

**Answer:** The current implementation serializes the event to JSON text and stores it in a `TEXT` column. This lets the publisher send the stored payload later without reconstructing the event from the trade entity. A JSON-specific database type could be considered if querying payload fields becomes a requirement.

## Q56. Do we need a foreign key from `outbox_event.aggregate_id` to `trade.trade_id`?

**Answer:** The current design uses a logical association through the trade identifier, and the transaction ensures the two records are saved together. A database foreign key is not assumed in the current schema. Whether to add one depends on lifecycle and schema requirements; do not claim one exists unless it is present in the actual schema.

------------------------------------------------------------------------

# Part 7 — Redis and Caching

## Q57. What is Redis?

**Answer:** Redis is an in-memory data store commonly used for caching, counters, coordination, and other low-latency data operations.

## Q58. Why use Redis in this project?

**Answer:** Many trades can reference the same symbol. Without caching, the processor may repeatedly call the Reference Data Service for identical information. Redis can hold reference data so subsequent lookups can avoid that network call.

## Q59. What is a cache hit?

**Answer:** A cache hit occurs when the requested key exists and a usable value is returned. The processor can use that value without calling the reference service.

## Q60. What is a cache miss?

**Answer:** A cache miss occurs when the key is absent or unusable. The processor fetches the data from the Reference Data Service and stores it in the cache for later lookups.

## Q61. What is our cache name and example key?

**Answer:** The current cache name is `referenceData`. An example key is `referenceData::AAPL`. The exact key convention should remain aligned with the Spring cache configuration.

## Q62. What are cache invalidation concerns?

**Answer:** Cached values can become stale when the source data changes. A real system needs an expiry policy, explicit invalidation, versioning, or another freshness strategy. The correct choice depends on how frequently reference data changes and how stale it can safely be.

## Q63. Does Redis replace PostgreSQL?

**Answer:** No. Redis is used as a cache in this project; PostgreSQL remains the primary database for trades and Outbox Events. Cache loss should not mean trade data is lost.

------------------------------------------------------------------------

# Part 8 — Reference Data and Service Boundaries

## Q64. Why separate the Reference Data Service?

**Answer:** Reference data has a distinct responsibility and can be managed behind a service API. The Trade Processor requests data by symbol instead of embedding the reference-data source directly into trade processing.

## Q65. What does the Reference Data API return?

**Answer:** The current response includes `symbol`, `exchange`, `currency`, and `sector`. The `symbol` field was added during development and should remain part of the documented contract.

## Q66. What if the Reference Data Service is unavailable?

**Answer:** The processor can fail the current processing attempt, and Spring Kafka’s retry/DLT mechanism handles the failure according to configuration. Whether a particular exception is retryable depends on the actual exception and retry configuration. If retries are exhausted, the record is routed to the DLT mechanism.

## Q67. Does an API response prove reference data is stored in PostgreSQL?

**Answer:** No. The API contract tells us what is returned, not how the service stores or retrieves it. The architecture proposes a `reference_data` table, but the current persistence implementation must be verified in the service source.

------------------------------------------------------------------------

# Part 9 — Idempotency and Duplicate Handling

## Q68. What is idempotency?

**Answer:** An operation is idempotent when repeating it has the same intended effect as performing it once. It is essential in systems where retries or redelivery can happen.

## Q69. How does the current application handle duplicate trades?

**Answer:** The consumer looks up the existing `tradeId` and skips processing if the trade already exists. The database unique constraint on `trade_id` is an additional safeguard.

## Q70. Is the application-level duplicate check alone sufficient?

**Answer:** No. A check followed by an insert is not atomic by itself. Concurrent deliveries could both pass the check. The unique database constraint is the final safeguard, and constraint-violation handling should be designed for concurrent processing.

## Q71. Does trade-level idempotency protect Notification Service side effects?

**Answer:** No. It prevents duplicate trade persistence in the Trade Processor, but it does not automatically prevent the same downstream event from being published or a notification from being sent twice. The planned `processed_events` design is intended to support downstream idempotency.

## Q72. Why does the planned `processed_events` table need a stable event ID?

**Answer:** A consumer needs a stable identifier to recognize that the same logical event has already been processed. A trade ID alone may not be enough if multiple distinct events can be emitted for the same trade over time.

------------------------------------------------------------------------

# Part 10 — Fee Calculation and Financial Correctness

## Q73. Have we implemented fee calculation?

**Answer:** Not yet. The current `trade` entity already includes `brokerage_fee` and `tax`, but the dedicated fee-calculation logic is planned. Do not claim that the application currently calculates these values unless the implementation has been added and verified.

## Q74. What fee calculation are we planning?

**Answer:** The proposed conceptual model is:

``` text
tradeValue       = price × quantity
brokerageFee     = tradeValue × brokerageRate
tax              = tradeValue × taxRate
regulatoryFee    = tradeValue × regulatoryRate
totalFees        = brokerageFee + tax + regulatoryFee
```

The rates, exact tax base, rounding rules, effective dates, and configuration model must be decided before implementation. The formulas are a starting design, not a complete financial rulebook.

## Q75. Why not hardcode fee rates?

**Answer:** Rates may change and can vary by market, instrument, trade side, or business rules. Configuration makes changes more manageable and auditable. The final model must be based on the actual requirements rather than assuming one universal rate.

## Q76. Why does rounding matter?

**Answer:** Financial calculations must follow defined precision and rounding rules. Rounding each component early can produce a different total from rounding only at the end. We need to agree on the scale, rounding mode, and calculation order before implementing fee logic.

## Q77. Which database changes are planned for fees?

**Answer:** The current entity already has `brokerage_fee` and `tax`. We are considering adding `regulatory_fee` and `total_fees`, and possibly a `fee_configuration` table. These changes are planned; the final schema depends on the fee requirements.

------------------------------------------------------------------------

# Part 11 — Spring Boot and Application Design

## Q78. Why use Spring Boot?

**Answer:** Spring Boot simplifies building Spring applications through auto-configuration, starter dependencies, embedded server support, and production-oriented integrations. It reduces setup work so the application can focus on its business workflow.

## Q79. What is dependency injection?

**Answer:** Dependency injection means an object receives its dependencies from a container rather than constructing them directly. Spring manages application components and injects collaborators, making components easier to configure, test, and replace.

## Q80. Why separate consumer, service, entity, and repository responsibilities?

**Answer:** Separation of concerns keeps each component focused. The consumer coordinates event handling, services own workflow or business operations, entities model persisted data, and repositories handle data access. This reduces coupling and improves testability.

## Q81. Why use a separate Outbox Publisher?

**Answer:** The publisher has a distinct responsibility: repeatedly find pending events and publish them. Separating it from the input consumer means the database transaction can complete even if Kafka is temporarily unavailable.

## Q82. What is a scheduled task?

**Answer:** A scheduled task runs according to a configured timing policy. The Outbox Publisher uses scheduling to poll for pending records. The current configuration has a 60-second initial delay and a 60-second fixed delay.

## Q83. What is the difference between `initialDelay` and `fixedDelay`?

**Answer:** `initialDelay` specifies how long to wait before the first execution. `fixedDelay` specifies the delay between the completion of one execution and the start of the next execution.

## Q84. Why use JSON serialization?

**Answer:** JSON provides a language-neutral format for event payloads. The consumer serializes the event for Outbox storage, and the publisher sends the stored payload to Kafka. The exact DTO and serialization configuration define the actual contract.

------------------------------------------------------------------------

# Part 12 — Docker, Maven, and Local Development

## Q85. Why use Docker Compose?

**Answer:** Docker Compose starts related local dependencies, such as Kafka, ZooKeeper where used, PostgreSQL, and Redis, with a repeatable configuration. It reduces machine-specific setup differences during development.

## Q86. Why is container networking important?

**Answer:** Applications running on the host and applications running inside containers may need different hostnames. For example, `localhost` inside a container refers to that container, not automatically to the host machine or another container. Kafka advertised listeners must match where the client runs.

## Q87. What is Maven used for?

**Answer:** Maven manages dependencies, builds the project, runs tests, and coordinates the multi-module build. The root `pom.xml` defines the project structure and shared build configuration.

## Q88. What is the purpose of a multi-module Maven project?

**Answer:** It organizes related services and shared code into modules with explicit dependencies. It supports a common build while keeping service responsibilities separated.

## Q89. Why use Git and GitHub for this project?

**Answer:** Git records incremental changes, while GitHub provides remote version control and a place to review code and documentation. Small, meaningful commits make progress easier to understand and regressions easier to trace.

## Q90. What should be checked before committing?

**Answer:** Review modified and untracked files, inspect the diff, run the relevant build/tests, and verify the application end-to-end when a code change affects runtime behavior. Documentation-only changes should still be checked for consistency and rendered correctly on GitHub.

------------------------------------------------------------------------

# Part 13 — Failure Scenarios and Trade-offs

## Q91. What happens if the trade and Outbox writes fail?

**Answer:** Both writes occur inside one database transaction. If the transaction rolls back, neither is committed. We tested this by injecting a temporary failure and verifying that neither row remained.

## Q92. What happens if Kafka is stopped during Outbox publication?

**Answer:** The publisher fails to send the event and leaves the Outbox row `PENDING`. After Kafka becomes available, a later scheduled run can send the event and mark it `PUBLISHED`. This recovery scenario has been tested.

## Q93. What happens if the input event is delivered twice?

**Answer:** The consumer checks the `tradeId`. If the trade already exists, it skips creating another trade and Outbox Event. The unique constraint protects the database against duplicate trade IDs.

## Q94. What is the difference between retry and Outbox retry?

**Answer:** Consumer retry handles a failure while processing an input Kafka record. Outbox retry handles a failure while publishing an already-persisted Outbox Event to downstream Kafka. They occur at different stages and protect against different failures.

## Q95. Why not simply catch every exception and continue?

**Answer:** Swallowing exceptions can acknowledge or skip failed work without a recovery path. Failures need explicit handling: retry transient issues, route exhausted processing failures to a DLT, roll back failed database transactions, and leave failed Outbox events pending.

## Q96. What operational features would a production version need?

**Answer:** Useful additions include metrics and alerts for consumer lag, retry/DLT volume, Outbox backlog age, publication failures, database health, and processing latency. It also needs clear replay procedures, schema evolution rules, secure configuration, and load testing.

------------------------------------------------------------------------

# Part 14 — Architecture and Scaling

## Q97. How would you scale the Trade Processor?

**Answer:** First measure throughput and identify the bottleneck. Potential approaches include increasing Kafka partitions, running more consumer instances, optimizing database access, batching where appropriate, and ensuring downstream services can handle the load. Consumer parallelism is constrained by partition count within a group.

## Q98. Is the project proven to support 1,000 trades per second?

**Answer:** Not merely because the requirements document lists 1,000 TPS. That is a target until measured through repeatable load tests with documented hardware, message sizes, database configuration, latency percentiles, and error rates.

## Q99. What are the risks of the current polling Outbox Publisher?

**Answer:** Polling can be simple and reliable for a portfolio application, but at larger scale it can repeatedly query the database, contend across publisher instances, and accumulate backlog. Future improvements could include indexed polling, batching, row claiming/locking, or change-data-capture approaches. These are trade-offs to evaluate rather than features already implemented.

## Q100. How would you prevent duplicate notifications in a future Notification Service?

**Answer:** Define a stable event ID and persist processed-event information so repeated delivery of the same event can be recognized. The design must also account for the boundary between recording the event and performing an external side effect, because a database transaction cannot normally atomically commit an email or SMS send.

## Q101. How would you evolve the downstream event contract?

**Answer:** Define a dedicated event DTO when the downstream consumer’s needs differ from the input `TradeEvent`. Add schema/version compatibility rules, include a stable event ID, and update producers, consumers, API contracts, and sequence diagrams together. The current Outbox payload is serialized from `TradeEvent`; a dedicated `ProcessedTradeEvent` remains a design option.

------------------------------------------------------------------------

# Part 15 — Testing and Verified Milestones

## Q102. Which important scenarios have we tested?

**Answer:**

- Happy path: trade and Outbox Event are saved and the publisher sends the event.
- Kafka failure and recovery: the Outbox remains `PENDING` while Kafka is unavailable, then becomes `PUBLISHED` after successful retry.
- Transaction atomicity: an intentional failure rolls back both the trade and Outbox writes.
- Duplicate delivery: sending the same trade event twice creates only one trade row and one Outbox row.
- Retry/DLT behavior: the injected transaction failure exhausted retries and reached the DLT handling path.

These tests establish the tested scenarios, not production-scale performance or universal correctness under every concurrency pattern.

## Q103. What is the most important lesson from the Outbox tests?

**Answer:** A reliability pattern should be verified through failure injection, not just a successful request. We checked the database state when Kafka was unavailable and when the database transaction was forced to fail. Those tests demonstrated the intended recovery and atomicity properties.

## Q104. What have we not tested or completed yet?

**Answer:** The project still needs planned fee calculation, full Notification Service processing, downstream `processed_events` idempotency, and further observability and performance work. The publisher crash window is an expected at-least-once trade-off and has not been forced as a separate crash test. Production-scale throughput has not been proven without load testing.

------------------------------------------------------------------------

# Part 16 — Interview Practice Framework

For design questions, structure the answer in this order:

1.  **Problem:** what failure or requirement are we addressing?
2.  **Choice:** which technology or pattern did we use?
3.  **Mechanism:** how does it work in our application?
4.  **Trade-off:** what limitation remains?
5.  **Verification:** how did we test or measure it?

Example: “Why the Transactional Outbox?”

- **Problem:** database save and Kafka publish can fail independently.
- **Choice:** Transactional Outbox.
- **Mechanism:** persist trade and Outbox Event in one PostgreSQL transaction; a scheduled publisher sends pending events.
- **Trade-off:** publication is at-least-once and can duplicate after a crash in the status-update window.
- **Verification:** tested Kafka-down recovery and transaction rollback.

## Suggested self-study order

1.  Project overview and end-to-end flow.
2.  Kafka fundamentals and delivery semantics.
3.  Transactional Outbox and its failure window.
4.  PostgreSQL transactions, constraints, and `BigDecimal`.
5.  Redis caching and failure handling.
6.  Retry/DLT and idempotency.
7.  Fee-calculation design.
8.  Scaling, observability, and production trade-offs.

------------------------------------------------------------------------

# Part 17 — Keep This Document Accurate

As the application changes:

- Mark a feature implemented only after it exists in code and has been verified.
- Update answers when topic names, DTOs, status values, or transaction boundaries change.
- Add questions about new implementation decisions and the trade-offs behind them.
- Keep planned architecture separate from completed behavior.
- Use `HLD.md`, `LLD.md`, `api-contracts.md`, `sequence-diagrams.md`, `database.md`, and `kafka-topics.md` as the project-specific reference documents.
