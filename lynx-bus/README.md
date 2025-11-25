# LynxBus – Kafka MVP & Full Feature Roadmap
*Inspired by Kafka: The Definitive Guide (O’Reilly)*

---

## 1. MVP Features (Beginner-Friendly Go Implementation)

### 1.1 Producer
- [x] Connect to Kafka brokers
- [x] Publish messages to a topic
- [ ] At-least-once delivery
- [ ] Maintain order per partition
- [ ] Retry on temporary failure
- [ ] Configurable:
    - [ ] Brokers
    - [ ] Topic
    - [ ] Timeout
    - [ ] Required acknowledgments (default: 1)
- [ ] Flush and close connections (`Close()`)
- [ ] Async internal implementation using goroutines & channels

### 1.2 Consumer
- [ ] Connect to Kafka brokers
- [ ] Join a consumer group
- [ ] Subscribe to a topic
- [ ] Poll messages asynchronously
- [ ] Deliver messages via callback
- [ ] Preserve order per partition
- [ ] Automatic offset commit
- [ ] Graceful shutdown:
    - [ ] Stop polling
    - [ ] Commit final offsets
    - [ ] Close connections

### 1.3 API (Go-idiomatic)
- Producer:
    - [ ] `NewProducer(config)`
    - [ ] `Publish(msg []byte)`
    - [ ] `Close()`
- Consumer:
    - [ ] `NewConsumer(config)`
    - [ ] `Listen(handler func(msg []byte))`

### 1.4 Testing & Examples
- [ ] Unit tests for producer & consumer
- [ ] Example producer application
- [ ] Example consumer application

---

## 2. Full Kafka Features (Post-MVP)

### 2.1 Advanced Producer Features
- [ ] Asynchronous publishing with callback
- [ ] Idempotent producer (exactly-once)
- [ ] Transactional messaging
- [ ] Message compression
- [ ] Batch publishing
- [ ] Custom partitioner

### 2.2 Advanced Consumer Features
- [ ] Manual offset commits
- [ ] Multiple topic subscriptions
- [ ] Rebalancing strategies
- [ ] Parallel message processing
- [ ] Dead-letter queues (DLQ)
- [ ] Filtering messages by key or headers

### 2.3 Cluster & Reliability Features
- [ ] Detect leader changes
- [ ] Handle broker failures and retries
- [ ] Fetch metadata periodically
- [ ] Partition reassignment handling

### 2.4 Schema & Serialization
- [ ] Support for Avro, Protobuf, JSON schemas
- [ ] Schema registry integration

### 2.5 Monitoring & Observability
- [ ] Metrics (throughput, latency, errors)
- [ ] Logging
- [ ] Tracing support

---

## 3. Roadmap
- **Phase 1**: MVP
    - Minimal producer and consumer with async internal handling
    - Auto-offset commit and retry
- **Phase 2**: Advanced features
    - Idempotent producer, batching, transactions
    - Parallel consumers, manual offset control
- **Phase 3**: Schema & observability
    - Schema registry, metrics, DLQ, tracing

---

## 4. Notes
- MVP must be **functional, safe, and simple**
- Full Kafka features can be added **incrementally**
- Go concurrency (goroutines + channels) enables async behavior from the start

