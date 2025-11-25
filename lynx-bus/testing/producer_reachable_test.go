// filepath: /home/florin/github/Go/lynx-bus/testing/producer_reachable_test.go
package testing

import (
	"sync"
	"testing"
	"time"

	"github.com/florin/lynx-bus/internal/broker"
)

func TestProducer_GivenNilReceiver_WhenAccessingState_ThenBehavesSafely(t *testing.T) {
	var p *broker.Producer

	if p.IsConnected() {
		t.Fatalf("expected not connected for nil receiver")
	}

	if got := p.Reachable(); got != nil {
		t.Fatalf("expected nil reachable for nil receiver, got: %v", got)
	}

	if err := p.Close(); err != nil {
		t.Fatalf("expected nil error from Close on nil receiver, got: %v", err)
	}
}

func TestProducer_GivenConnectedProducer_WhenClose_ThenClearsReachableAndIdempotent(t *testing.T) {
	addr, closeFn := StartKafkaMock_Valid(t)
	defer closeFn()

	cfg := NewProducerConfig([]string{addr}, "t", 2*time.Second)
	p, err := broker.NewProducer(cfg)
	if err != nil {
		t.Fatalf("expected success, got error: %v", err)
	}
	// Ensure connected initially
	if !p.IsConnected() {
		_ = p.Close()
		t.Fatalf("expected connected before Close")
	}

	// Close once
	if err := p.Close(); err != nil {
		t.Fatalf("unexpected error on Close: %v", err)
	}

	// Close again to verify idempotence
	if err := p.Close(); err != nil {
		t.Fatalf("unexpected error on second Close: %v", err)
	}

	if p.IsConnected() {
		t.Fatalf("expected not connected after Close")
	}

	if got := p.Reachable(); len(got) != 0 {
		t.Fatalf("expected empty reachable after Close, got: %v", got)
	}
}

func TestProducer_GivenConcurrentReaders_WhenClose_ThenSafeAndClearsReachable(t *testing.T) {
	addr, closeFn := StartKafkaMock_Valid(t)
	defer closeFn()

	cfg := NewProducerConfig([]string{addr}, "t", 2*time.Second)
	p, err := broker.NewProducer(cfg)
	if err != nil {
		t.Fatalf("expected success, got error: %v", err)
	}
	// Ensure connected before stress
	if !p.IsConnected() {
		_ = p.Close()
		t.Fatalf("expected connected before concurrent test")
	}

	var wg sync.WaitGroup
	done := make(chan struct{})

	// Spawn several readers that call IsConnected/Reachable repeatedly.
	readers := 4
	wg.Add(readers)
	for i := 0; i < readers; i++ {
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
					_ = p.IsConnected()
					_ = p.Reachable()
					time.Sleep(5 * time.Millisecond)
				}
			}
		}()
	}

	// Let readers run briefly, then close producer concurrently.
	time.Sleep(50 * time.Millisecond)
	if err := p.Close(); err != nil {
		t.Fatalf("unexpected error on Close during concurrent access: %v", err)
	}

	// Stop readers and wait for them to finish.
	close(done)
	wg.Wait()

	// After Close, producer should report not connected and empty reachable.
	if p.IsConnected() {
		t.Fatalf("expected not connected after Close")
	}
	if got := p.Reachable(); len(got) != 0 {
		t.Fatalf("expected empty reachable after Close, got: %v", got)
	}
}
