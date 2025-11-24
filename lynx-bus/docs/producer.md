# Producer Documentation

## Overview
The `Producer` is a minimal implementation designed to verify connectivity to brokers. It does not use any Kafka client libraries and performs TCP-level connection checks as part of the MVP.

## Producer Configuration
The `ProducerConfig` struct holds the configuration for the `Producer`:

- **Brokers**: A list of broker addresses.
- **Topic**: The topic to connect to.
- **DialTimeout**: Timeout duration for TCP connections.

Example:
```go
ProducerConfig{
    Brokers:     []string{"broker1:9092", "broker2:9092"},
    Topic:       "example-topic",
    DialTimeout: 5 * time.Second,
}
```

## Producer Implementation
The `Producer` struct includes:

- **cfg**: The configuration for the producer.
- **reachable**: A list of reachable brokers.
- **reachableMu**: A mutex for thread-safe access to the reachable list.
- **lastChecked**: The last time the connectivity was checked.

### Key Methods

#### `NewProducer`
Creates a new `Producer` instance by checking connectivity to the configured brokers. At least one reachable broker is required for success.

Example:
```go
cfg := ProducerConfig{
    Brokers:     []string{"broker1:9092"},
    Topic:       "example-topic",
    DialTimeout: 5 * time.Second,
}
producer, err := NewProducer(cfg)
if err != nil {
    log.Fatalf("Failed to create producer: %v", err)
}
```

## Testing
The `producer_test.go` file contains unit tests for the `Producer`:

### Test Cases

#### 1. Valid Broker Connection
Verifies that the `Producer` successfully connects to a valid broker.

```go
func TestNewProducer_GivenValidBroker_WhenConnecting_ThenShouldSucceed(t *testing.T) {
    addr, closeFn := StartKafkaMock_Valid(t)
    defer closeFn()

    cfg := NewProducerConfig([]string{addr}, "t", 2*time.Second)
    p, err := broker.NewProducer(cfg)
    if err != nil {
        t.Fatalf("expected success, got error: %v", err)
    }
    defer p.Close()

    if !p.IsConnected() {
        t.Fatalf("expected connected")
    }
}
```

#### 2. Unreachable TCP
Tests the behavior when the broker's TCP address is unreachable.

```go
func TestNewProducer_GivenUnreachableTCP_WhenConnecting_ThenShouldFail(t *testing.T) {
    addr := GetFreeAddr(t)
    cfg := NewProducerConfig([]string{addr}, "t", 500*time.Millisecond)

    p, err := broker.NewProducer(cfg)
    if err == nil {
        t.Fatalf("expected error for unreachable tcp, got success")
    }
}
```

#### 3. Non-Kafka Speaker
Checks that the `Producer` fails when the broker is not a Kafka speaker.

```go
func TestNewProducer_GivenNonKafkaSpeaker_WhenConnecting_ThenShouldFail(t *testing.T) {
    addr, closeFn := StartKafkaMock_Invalid(t)
    defer closeFn()

    cfg := NewProducerConfig([]string{addr}, "t", 2*time.Second)
    p, err := broker.NewProducer(cfg)
    if err == nil {
        t.Fatalf("expected error for non-kafka speaker, got success")
    }
}
```

## Conclusion
The `Producer` implementation provides a lightweight mechanism to verify broker connectivity. The accompanying tests ensure its reliability under various scenarios.
