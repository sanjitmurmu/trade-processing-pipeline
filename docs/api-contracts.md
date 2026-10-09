# API Contracts

## Trade Processing Pipeline

This document defines the communication contracts between the Trade Processing Pipeline’s HTTP endpoints and Kafka-based components.

It distinguishes the contracts established by the current project design from details that must be verified against the current source code. Do not treat an example response or an unimplemented downstream event as an implemented contract.

------------------------------------------------------------------------

# 1. Contract Inventory

| Interface                      | Direction                                      | Purpose                            | Status                                                      |
|--------------------------------|------------------------------------------------|------------------------------------|-------------------------------------------------------------|
| Trade submission API           | Client → Trade Producer                        | Submit a trade for processing      | Implemented; verify exact HTTP response against controller  |
| Trade input event              | Trade Producer → Kafka `trade-events`          | Deliver a trade to the processor   | Implemented                                                 |
| Reference-data API             | Trade Processor → Reference Data Service       | Retrieve enrichment data by symbol | Implemented; response includes `symbol`                     |
| Outbox downstream event        | Outbox Publisher → Kafka `trade-notifications` | Publish the stored event payload   | Implemented; payload currently serialized from `TradeEvent` |
| Notification consumer contract | Kafka → Notification Service                   | Consume downstream events          | Planned / not fully implemented                             |
| Fee-calculation contract       | Trade Processor → fee calculation component    | Calculate fees                     | Planned                                                     |

------------------------------------------------------------------------

# 2. Trade Submission API

## 2.1 Purpose

The Trade Producer exposes an HTTP API for clients to submit trades. The producer publishes the accepted trade event to Kafka topic `trade-events`.

The original design specifies:

``` http
POST /trade
```

Confirm the exact controller mapping and response status/body against the current `trade-producer` source before treating undocumented response details as final.

## 2.2 Request

**Content-Type:** `application/json`

Example request:

``` http
POST /trade
Content-Type: application/json
```

``` json
{
  "tradeId": "TR123",
  "symbol": "AAPL",
  "quantity": 100,
  "price": 220,
  "side": "BUY"
}
```

This example reflects the trade fields used in the project design. The actual request DTO in source is authoritative if its fields or validation rules differ.

### Field reference

| Field      | Type           | Meaning                     | Notes                                    |
|------------|----------------|-----------------------------|------------------------------------------|
| `tradeId`  | string         | Trade identifier            | Used as the trade-level idempotency key  |
| `symbol`   | string         | Instrument symbol           | Used to retrieve reference data          |
| `quantity` | integer        | Number of units             | Intended to be positive                  |
| `price`    | decimal/number | Price per unit              | Intended to be positive                  |
| `side`     | string/enum    | Trade direction, e.g. `BUY` | Must match the application’s enum values |

The request example is not a substitute for source-level validation rules. Required fields, enum values, numeric constraints, and error responses should be synchronized with the actual controller and DTO.

## 2.3 Response

The exact HTTP response status and body are not specified in the design material available for this document. They must be confirmed from the current controller implementation rather than invented here.

Document the following from source when verified:

- Successful response status.
- Response body and content type.
- Whether the response means only that the event was accepted/published or that processing has completed.
- Validation and serialization error responses.

**Important:** because processing continues asynchronously through Kafka, an HTTP success response should not automatically be described as confirmation that the trade has been persisted by the Trade Processor unless the implementation explicitly guarantees that.

------------------------------------------------------------------------

# 3. Trade Input Kafka Event

## 3.1 Topic and ownership

| Property       | Value                   |
|----------------|-------------------------|
| Topic          | `trade-events`          |
| Producer       | Trade Producer          |
| Consumer       | Trade Processor         |
| Consumer group | `trade-processor-group` |
| Message key    | `tradeId`               |
| Payload format | JSON                    |

## 3.2 Payload

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

The Trade Producer publishes the trade event to `trade-events`. The Trade Processor consumes the event and performs duplicate detection, reference-data enrichment, and transactional persistence.

The actual shared `TradeEvent` DTO is the source of truth for field names, Java types, nullability, and serialization behavior.

## 3.3 Message key and ordering

The message key is the trade identifier:

``` text
key = tradeId
```

Kafka routes messages with the same key to the same partition under a stable partitioning configuration. This supports per-key ordering within that partition. It does not imply a global ordering guarantee across all trades or across arbitrary partition-count changes.

## 3.4 Processing semantics

The event is processed asynchronously. The current consumer checks for an existing `tradeId` before continuing.

The trade and its Outbox Event are then written within one PostgreSQL transaction. Kafka publication of the downstream Outbox Event happens separately through the Outbox Publisher.

------------------------------------------------------------------------

# 4. Reference Data API

## 4.1 Purpose

The Reference Data Service returns enrichment information for a symbol. The Trade Processor uses the result to populate the trade’s exchange, currency, and sector fields.

## 4.2 Endpoint

The design specifies:

``` http
GET /reference/{symbol}
```

Example:

``` http
GET /reference/AAPL
```

Confirm any service base path or port separately from this path. Local service ports are environment configuration, not part of the resource path.

## 4.3 Successful response

The current response contract includes the requested symbol:

``` http
HTTP/1.1 200 OK
Content-Type: application/json
```

``` json
{
  "symbol": "AAPL",
  "exchange": "NASDAQ",
  "currency": "USD",
  "sector": "TECH"
}
```

### Response fields

| Field      | Type   | Meaning                                 |
|------------|--------|-----------------------------------------|
| `symbol`   | string | Instrument symbol                       |
| `exchange` | string | Exchange where the instrument is listed |
| `currency` | string | Currency associated with the instrument |
| `sector`   | string | Instrument sector classification        |

**Implementation note:** `symbol` was added to the response during development. Keep it in this contract and in the corresponding response DTO.

## 4.4 Cache behavior

The Trade Processor uses Redis caching for reference data.

The current cache name is:

``` text
referenceData
```

Example cache key:

``` text
referenceData::AAPL
```

The cache is an internal optimization. It does not change the public response shape of the Reference Data API.

## 4.5 Error responses

The exact error status codes and response body for an unknown symbol, invalid symbol, or Reference Data Service failure must be verified against the current controller and exception handling. This document does not prescribe a response format that has not been implemented.

The Trade Processor’s retry/DLT behavior is documented separately from the HTTP error response contract.

------------------------------------------------------------------------

# 5. Outbox Downstream Kafka Event

## 5.1 Topic

The current Outbox Event configuration publishes to:

``` text
trade-notifications
```

The older design used `trade-processed`. That is historical design terminology, not the current topic configured by the Outbox implementation.

## 5.2 Current payload behavior

The current consumer serializes a `TradeEvent` payload and stores it in the `OutboxEvent`. The Outbox Publisher sends the stored payload to the topic recorded on that Outbox Event.

Example payload shape:

``` json
{
  "tradeId": "TR123",
  "symbol": "AAPL",
  "quantity": 100,
  "price": 220,
  "side": "BUY"
}
```

This example shows the current serialization source and should not be interpreted as a finalized business event schema containing calculated fees or a persisted-trade status.

## 5.3 Delivery and status behavior

The Outbox Event currently uses these statuses:

``` text
PENDING
PUBLISHED
```

Publication flow:

1.  The Outbox Publisher selects `PENDING` events.
2.  It sends the payload to the event’s configured Kafka topic.
3.  It waits for the Kafka send result.
4.  On success, it marks the event `PUBLISHED` and sets `publishedAt`.
5.  On failure, the event remains `PENDING` for a later scheduled attempt.

This is an at-least-once publication approach. A crash after Kafka accepts a message but before the database status update can lead to duplicate publication.

## 5.4 Contract to finalize before Notification Service implementation

Before implementing the Notification Service, decide whether its input should remain the current serialized `TradeEvent` or become a dedicated downstream DTO such as the originally proposed `ProcessedTradeEvent`.

A finalized downstream contract should define:

- Event name/type.
- Stable event identifier for downstream idempotency.
- Trade identifier.
- Required trade and enrichment fields.
- Fee fields once fee calculation exists.
- Schema evolution and compatibility expectations.

Until that decision is implemented, do not describe `ProcessedTradeEvent` as the current published contract.

------------------------------------------------------------------------

# 6. Planned Fee Fields and Future Contract

Fee calculation is planned and is not yet part of the current input or Outbox payload contract.

The intended calculation model is:

``` text
tradeValue       = price × quantity
brokerageFee     = tradeValue × brokerageRate
tax              = tradeValue × taxRate
regulatoryFee    = tradeValue × regulatoryRate
totalFees        = brokerageFee + tax + regulatoryFee
```

After the fee-calculation implementation, the downstream event contract may need to include the calculated values:

``` json
{
  "tradeId": "TR123",
  "symbol": "AAPL",
  "quantity": 100,
  "price": 220,
  "side": "BUY",
  "brokerageFee": "<calculated value>",
  "tax": "<calculated value>",
  "regulatoryFee": "<calculated value>",
  "totalFees": "<calculated value>"
}
```

This is a **future illustration only**. It is not a valid current payload example, and placeholder values are deliberately used instead of fabricated amounts. The final field names, decimal representation, precision, rounding rules, and event schema must be agreed during implementation.

------------------------------------------------------------------------

# 7. Error and Failure Contract Boundaries

The system has distinct failure boundaries.

| Failure point                                  | Current/expected handling                                               | Contract status                            |
|------------------------------------------------|-------------------------------------------------------------------------|--------------------------------------------|
| Trade submission request is malformed          | Depends on Trade Producer controller/validation                         | Verify exact HTTP response from source     |
| Trade Producer cannot publish input event      | Depends on producer send/error handling                                 | Verify exact behavior from source          |
| Trade processing fails                         | Spring Kafka retry handling; DLT after configured retries are exhausted | Implemented                                |
| Trade or Outbox database write fails           | Transaction rolls back both writes                                      | Implemented and tested                     |
| Kafka is unavailable during Outbox publication | Outbox event remains `PENDING` for a later attempt                      | Implemented and tested                     |
| Duplicate trade event is received              | Existing trade is skipped; unique constraint protects the table         | Implemented and tested                     |
| Reference Data API fails                       | Consumer retry/DLT behavior applies to processing failure               | Exact HTTP error response must be verified |
| Notification processing fails                  | No complete downstream contract yet                                     | Planned                                    |

Do not conflate an HTTP error response from a service with Kafka retry/DLT behavior. They are different interfaces and may have different error formats.

------------------------------------------------------------------------

# 8. Contract Compatibility Guidelines

As the project evolves:

1.  Treat shared DTOs in source as authoritative for current field names and types.
2.  Update this document when a request, response, topic, message key, or payload changes.
3.  Preserve backward compatibility where possible when changing event fields.
4.  Add new optional fields before making consumers depend on them.
5.  Decide how unknown fields, missing fields, and enum changes are handled.
6.  Keep HTTP response documentation separate from Kafka event documentation.
7.  Do not mark a proposed future payload as implemented until the producer and consumer agree on it.
8.  Update `sequence-diagrams.md` and `database.md` whenever a contract change affects processing or persistence.

------------------------------------------------------------------------

# 9. Items to Verify Against Source

The following details should be filled in after checking the current controller, DTO, and configuration classes:

- Exact Trade Producer success response status and body.
- Exact Trade Producer validation/error response format.
- Exact Reference Data error responses.
- Exact required/nullability constraints on `TradeEvent`.
- Exact numeric Java type and JSON representation for `price`.
- Whether producer publication waits for Kafka acknowledgement before responding to the HTTP caller.
- Final downstream event DTO and stable event ID strategy before Notification Service implementation.

These items are intentionally called out rather than guessed. Once verified, update this document with the actual implementation details.
