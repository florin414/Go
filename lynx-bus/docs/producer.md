# Producer — Design & Implementation (internal/broker)

This document summarizes the current state of the minimal Producer implementation under internal/broker and the related test helpers. It is written to make the code structure and wire-format decisions explicit so you can quickly understand what was changed and why.

## Overview
- **Purpose**: a lightweight, dependency-free producer used to verify broker connectivity and to send simple framed Produce requests (MVP). It does not pull in a full Kafka client library — instead it implements only the small set of wire-level operations required by tests and the demo.
- **Location**: internal/broker

## Project layout (relevant files)
- `producer.go` — NewProducer and core connectivity checks.
- `reachable.go` — Producer.Reachable() (returns a copy of reachable brokers).
- `isconnected.go` — Producer.IsConnected() (reports if any broker was reachable).
- `close.go` — Producer.Close() (cleans up/marks closed; idempotent and nil-safe).
- `api_versions.go` — ApiVersions probe implementation and request builder helpers.
- `encoding.go` — Produce request builder, MessageSet builder and related encoding logic.
- `binary_helpers.go` — Shared, centralized binary read/write helpers used by both api_versions.go and encoding.go.
- `publish.go` — high-level Publish method (uses encoding builders and dialer) — (not part of this document in detail but covered in tests).

## Key design decisions
1. **Avoid magic numbers / centralize defaults**
- Protocol-specific constants (api keys/versions, magic bytes, min response sizes) were removed from inline literals and are configurable via small config structs and Default* constructors.
- This makes the wire-format explicit and easy to override for tests.

2. **Explicit binary helpers**
- All binary reads/writes are done via small helpers (`writeInt16`, `writeInt32`, `writeInt64`, `writeKafkaString`, `readInt16`, `readInt32`, `readKafkaString`, etc.) rather than using reflection-based `binary.Write` directly at call sites.
- Helpers are collected in `binary_helpers.go` so encoding/decoding rules are implemented once and shared by mocks/tests and the production code.

3. **Separate concerns and small files**
- Small related functions were moved to their own files (`Reachable`/`IsConnected`/`Close` each in their own file) to improve discoverability.
- The ApiVersions logic is split into a `DefaultApiVersionsConfig` + a `checkApiVersionsWithConfig` implementation so callers can use defaults or provide explicit configs for tests.

## Wire formats (explicit)
1. **ApiVersions probe (what the code currently sends)**
- Frame: 4-byte big-endian int32 = payload length
- Payload: `apiKey(int16)` | `apiVersion(int16)` | `correlationId(int32)` | `clientIdLength(int16)` | `clientId(bytes)`
- Response (in the simplified probe used here): framed payload whose first 4 payload bytes are the correlation id echoed back. The mock servers rely on that behavior for a lightweight check.

2. **ProduceRequest v0 (what buildProduceRequest builds)**
- Request header (inside the framed request): `apiKey(int16)` | `apiVersion(int16)` | `correlationId(int32)` | `clientId(kafka-string)`
- Payload body (ProduceRequest v0 payload): `RequiredAcks(int16)` | `Timeout(int32)` | `TopicsArrayCount(int32)` |
  [ `Topic(kafka-string)` | `PartitionsArrayCount(int32)` | [ `Partition(int32)` | `MessageSet(bytes)` ] ]

3. **MessageSet (single message in tests)**
- MessageSet entry: `offset(int64)` | `messageSize(int32)` | `message`
- `message` = `CRC(uint32)` | `magic(int8)` | `attributes(int8)` | `key(bytes nullable int32)` | `value(bytes nullable int32)`

## What the config types provide
- `ApiVersionsConfig` + `DefaultApiVersionsConfig()`
  - Network (e.g. "tcp"), ClientID, CorrelationID, ApiKey, ApiVersion, MinResponseSize, Dialer
  - `checkApiVersions(addr, timeout)` uses defaults; `checkApiVersionsWithConfig` allows overrides.

- `ProduceConfig` + `DefaultProduceConfig()`
  - ApiKey, ApiVersion, CorrelationID, ClientID — used by `buildProduceRequestWithConfig` to create framed Produce requests.

## Binary helpers (centralized)
- Functions like `writeInt16`/`writeInt32`/`writeInt64`/`writeUint32`/`writeBytesWithInt32Len`, `writeKafkaString`, `readInt16`, `readInt32`, `readKafkaString`, `parseCorrelationFromBytes` are implemented in `binary_helpers.go` and shared across files.
- This prevents duplicate implementations and ensures tests and code use identical framing.

## Testing & Mocks
- Tests follow a BDD-style naming convention (`Given_When_Then`), e.g. `TestNewProducer_GivenValidBroker_WhenConnecting_ThenShouldSucceed` and `TestPublish_GivenValidBroker_WhenPublishing_ThenSucceeds`.
- Mock servers (`testing/mock.go`):
  - `StartKafkaMock_Valid` / `StartKafkaMock_Invalid` — simple ApiVersions mocks used by NewProducer tests.
  - `StartKafkaMock_Produce_Success` / `StartKafkaMock_Produce_Error` — handle both ApiVersionsRequest (apiKey=18) and ProduceRequest (apiKey=0). They return framed ProduceResponse payloads used by Publish tests.
- Utilities: `GetFreeAddr` and `NewProducerConfig` are provided in `testing/util.go` and `testing/factory.go` to simplify setup.

## Publish tests
- New publish tests were added under `testing/publish_test.go` and use the produce-aware mocks to exercise Publish end-to-end over TCP.
- Scenarios covered: successful publish, nil producer, empty value validation, server-side produce error, publish after Close (no reachable brokers), dial failures.

## Concurrency & safety
- `Reachable`/`IsConnected`/`Close` are implemented using a `RWMutex`. `Close` clears the reachable slice while holding the write lock; readers use `RLock`. There are unit tests that spawn concurrent readers while `Close` runs to validate safety.

## How to read the code quickly
1. Start with `binary_helpers.go` to understand how bytes are encoded/decoded. This shows endianness and nullable conventions.
2. Read `api_versions.go` for the probe flow (`DefaultApiVersionsConfig` -> `buildApiVersionsRequest` -> `checkApiVersionsWithConfig`). The check is intentionally lightweight.
3. Read `encoding.go` to see Produce request building and MessageSet structure.
4. Read `publish.go` to see how the request is sent and responses parsed. Tests and mocks show the expected message shapes.
5. Look at `reachable.go` / `isconnected.go` / `close.go` for consumer-facing small helpers.

## Suggested next documentation improvements
- Add a small sequence diagram showing framing and offset positions (correlation id location at payload bytes 4..7 is a common source of confusion).
- Document the minimal ApiVersionsResponse parser (currently the probe only validates correlation id); if you implement full negotiation, document how min/max versions are interpreted.
- Add byte-level unit tests: assert that `buildApiVersionsRequest` and `buildProduceRequest` produce the exact expected bytes for a known config.

## Quick commands
- Run tests: `go test ./...` (the testing/ package contains the tests)

If you want, I can:
- Add a short sequence diagram to this file (ASCII art) showing the request/response offsets.
- Produce a minimal ApiVersionsResponse parser and update NewProducer to use it.
- Add byte-level golden tests asserting exact wire bytes for requests.

Tell me which of the above you'd like next and I'll implement it.
