// filepath: /home/florin/github/Go/lynx-bus/testing/publish_test.go
package testing

import (
	"strings"
	"testing"
	"time"

	"LynxBus/internal/broker"
)

func TestPublish_GivenValidBroker_WhenPublishing_ThenSucceeds(t *testing.T) {
	addr, closeMock := StartKafkaMock_Produce_Success(t)
	defer closeMock()

	cfg := NewProducerConfig([]string{addr}, "t", 2*time.Second)
	p, err := broker.NewProducer(cfg)
	if err != nil {
		t.Fatalf("NewProducer failed: %v", err)
	}
	defer func() { _ = p.Close() }()

	if err := p.Publish(nil, []byte("hello")); err != nil {
		t.Fatalf("expected publish success, got error: %v", err)
	}
}

func TestPublish_GivenNilProducer_WhenPublishing_ThenReturnsProducerNilError(t *testing.T) {
	var p *broker.Producer
	err := p.Publish(nil, []byte("x"))
	if err == nil || !strings.Contains(err.Error(), "producer is nil") {
		t.Fatalf("expected producer is nil error, got: %v", err)
	}
}

func TestPublish_GivenEmptyValue_WhenPublishing_ThenReturnsValueRequiredError(t *testing.T) {
	addr, closeMock := StartKafkaMock_Produce_Success(t)
	defer closeMock()

	cfg := NewProducerConfig([]string{addr}, "t", 2*time.Second)
	p, err := broker.NewProducer(cfg)
	if err != nil {
		t.Fatalf("NewProducer failed: %v", err)
	}
	defer func() { _ = p.Close() }()

	err = p.Publish(nil, []byte{})
	if err == nil || !strings.Contains(err.Error(), "value required") {
		t.Fatalf("expected value required error, got: %v", err)
	}
}

func TestPublish_GivenBrokerReturnsError_WhenPublishing_ThenReturnsProduceError(t *testing.T) {
	addr, closeMock := StartKafkaMock_Produce_Error(t, int16(5))
	defer closeMock()

	cfg := NewProducerConfig([]string{addr}, "t", 2*time.Second)
	p, err := broker.NewProducer(cfg)
	if err != nil {
		t.Fatalf("NewProducer failed: %v", err)
	}
	defer func() { _ = p.Close() }()

	err = p.Publish(nil, []byte("msg"))
	if err == nil || !strings.Contains(err.Error(), "produce error") {
		t.Fatalf("expected produce error, got: %v", err)
	}
}

func TestPublish_GivenClosedProducer_WhenPublishing_ThenNoReachableBrokers(t *testing.T) {
	addr, closeMock := StartKafkaMock_Produce_Success(t)
	defer closeMock()

	cfg := NewProducerConfig([]string{addr}, "t", 2*time.Second)
	p, err := broker.NewProducer(cfg)
	if err != nil {
		t.Fatalf("NewProducer failed: %v", err)
	}

	// close producer which clears reachable
	if err := p.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	err = p.Publish(nil, []byte("msg"))
	if err == nil || !strings.Contains(err.Error(), "no reachable brokers") {
		t.Fatalf("expected no reachable brokers error after Close, got: %v", err)
	}
}

func TestPublish_GivenBrokerClosed_WhenPublishing_ThenDialFailure(t *testing.T) {
	addr, closeMock := StartKafkaMock_Produce_Success(t)
	// we'll close the mock immediately after creating producer to force dial failure
	cfg := NewProducerConfig([]string{addr}, "t", 500*time.Millisecond)
	p, err := broker.NewProducer(cfg)
	if err != nil {
		closeMock()
		t.Fatalf("NewProducer failed: %v", err)
	}
	// close the mock to make subsequent dials fail
	closeMock()
	defer func() { _ = p.Close() }()

	err = p.Publish(nil, []byte("msg"))
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "dial") {
		t.Fatalf("expected dial error, got: %v", err)
	}
}
