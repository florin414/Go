package testing

import (
	"testing"
	"time"

	"LynxBus/internal/broker"
)

func TestNewProducer_GivenValidBroker_WhenConnecting_ThenShouldSucceed(t *testing.T) {
	addr, closeFn := StartKafkaMock_Valid(t)
	defer closeFn()

	cfg := NewProducerConfig([]string{addr}, "t", 2*time.Second)
	p, err := broker.NewProducer(cfg)
	if err != nil {
		t.Fatalf("expected success, got error: %v", err)
	}
	defer func() { _ = p.Close() }()

	if !p.IsConnected() {
		t.Fatalf("expected connected")
	}
	reach := p.Reachable()
	if len(reach) != 1 || reach[0] != addr {
		t.Fatalf("unexpected reachable: %v", reach)
	}
}

func TestNewProducer_GivenUnreachableTCP_WhenConnecting_ThenShouldFail(t *testing.T) {
	addr := GetFreeAddr(t)
	cfg := NewProducerConfig([]string{addr}, "t", 500*time.Millisecond)

	p, err := broker.NewProducer(cfg)
	if err == nil {
		_ = p.Close()
		t.Fatalf("expected error for unreachable tcp, got success")
	}
	_ = p
}

func TestNewProducer_GivenNonKafkaSpeaker_WhenConnecting_ThenShouldFail(t *testing.T) {
	addr, closeFn := StartKafkaMock_Invalid(t)
	defer closeFn()

	cfg := NewProducerConfig([]string{addr}, "t", 2*time.Second)
	p, err := broker.NewProducer(cfg)
	if err == nil {
		_ = p.Close()
		t.Fatalf("expected error for non-kafka speaker, got success")
	}
	_ = p
}

func TestNewProducer_GivenMixedBrokers_WhenOneIsValid_ThenShouldSucceed(t *testing.T) {
	good, closeGood := StartKafkaMock_Valid(t)
	defer closeGood()

	bad, closeBad := StartKafkaMock_Invalid(t)
	defer closeBad()

	cfg := NewProducerConfig([]string{bad, good}, "t", 3*time.Second)
	p, err := broker.NewProducer(cfg)
	if err != nil {
		t.Fatalf("expected success when one broker speaks Kafka, got error: %v", err)
	}
	defer func() { _ = p.Close() }()

	if !p.IsConnected() {
		t.Fatalf("expected connected")
	}

	reach := p.Reachable()
	if len(reach) != 1 || reach[0] != good {
		t.Fatalf("expected only good broker reachable, got: %v", reach)
	}
}
