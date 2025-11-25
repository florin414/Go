package broker

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"
)

// ProducerConfig holds minimal configuration for broker connectivity checks.
type ProducerConfig struct {
	Brokers     []string
	Topic       string
	DialTimeout time.Duration
}

// Producer represents a minimal producer that verifies connectivity to brokers.
// This implementation does not use any Kafka client libraries and only performs
// TCP-level connection checks as the first step of the MVP.
type Producer struct {
	cfg         ProducerConfig
	reachable   []string
	reachableMu sync.RWMutex
	lastChecked time.Time
}

// NewProducer checks broker connectivity (TCP dial) and returns a Producer.
// It attempts to connect to each configured broker within the DialTimeout.
// At least one reachable Kafka-speaking broker is required for success.
func NewProducer(cfg ProducerConfig) (*Producer, error) {
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("no brokers provided")
	}
	if cfg.DialTimeout == 0 {
		cfg.DialTimeout = 5 * time.Second
	}

	p := &Producer{cfg: cfg}

	ctx, cancel := context.WithTimeout(context.Background(), cfg.DialTimeout)
	defer cancel()

	type result struct {
		addr string
		err  error
	}

	ch := make(chan result, len(cfg.Brokers))

	for _, b := range cfg.Brokers {
		addr := b
		go func() {
			d := cfg.DialTimeout
			// allow per-dial context deadline shorter than overall ctx
			conn, err := net.DialTimeout("tcp", addr, d)
			if err == nil {
				_ = conn.Close()
			}
			select {
			case ch <- result{addr: addr, err: err}:
			case <-ctx.Done():
			}
		}()
	}

	var tcpReachable []string
	for i := 0; i < len(cfg.Brokers); i++ {
		select {
		case res := <-ch:
			if res.err == nil {
				tcpReachable = append(tcpReachable, res.addr)
			}
		case <-ctx.Done():
			// timeout waiting for remaining brokers
			break
		}
	}

	if len(tcpReachable) == 0 {
		return nil, fmt.Errorf("no reachable brokers from %v (timeout %s)", cfg.Brokers, cfg.DialTimeout)
	}

	// Perform a lightweight Kafka protocol check (ApiVersionsRequest v0) on the TCP-reachable brokers
	// to ensure the peer speaks Kafka and responds correctly. The actual checkApiVersions
	// implementation lives in api_versions.go.
	var kafkaSpeakers []string
	for _, addr := range tcpReachable {
		// compute remaining time from context deadline for per-broker check
		var perTimeout time.Duration = 2 * time.Second
		if dl, ok := ctx.Deadline(); ok {
			rem := time.Until(dl)
			if rem < perTimeout && rem > 0 {
				perTimeout = rem
			}
		}
		if err := checkApiVersions(addr, perTimeout); err == nil {
			kafkaSpeakers = append(kafkaSpeakers, addr)
		}
	}

	if len(kafkaSpeakers) == 0 {
		return nil, fmt.Errorf("no Kafka-speaking brokers found among reachable addresses %v", tcpReachable)
	}

	p.reachableMu.Lock()
	p.reachable = kafkaSpeakers
	p.lastChecked = time.Now()
	p.reachableMu.Unlock()

	return p, nil
}
