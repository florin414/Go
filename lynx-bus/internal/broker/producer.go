package broker

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
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
	// to ensure the peer speaks Kafka and responds correctly.
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

// checkApiVersions performs a minimal ApiVersionsRequest (version 0) to verify the
// peer speaks the Kafka protocol. It sends a request with correlation id 1 and
// ensures the response contains the same correlation id. This is implemented
// from scratch: framing + request header encoding using big-endian integers.
func checkApiVersions(addr string, timeout time.Duration) error {
	// ApiVersions API key is 18, version 0 has no request body.
	const apiKey int16 = 18
	const apiVersion int16 = 0
	const correlationId int32 = 1
	clientID := "lynxbus"

	buf := &bytes.Buffer{}
	// request header: apiKey(int16), apiVersion(int16), correlationId(int32), clientId(string)
	if err := binary.Write(buf, binary.BigEndian, apiKey); err != nil {
		return err
	}
	if err := binary.Write(buf, binary.BigEndian, apiVersion); err != nil {
		return err
	}
	if err := binary.Write(buf, binary.BigEndian, correlationId); err != nil {
		return err
	}
	if err := binary.Write(buf, binary.BigEndian, int16(len(clientID))); err != nil {
		return err
	}
	if _, err := buf.WriteString(clientID); err != nil {
		return err
	}

	payload := buf.Bytes()
	// prepend length (int32)
	out := &bytes.Buffer{}
	if err := binary.Write(out, binary.BigEndian, int32(len(payload))); err != nil {
		return err
	}
	if _, err := out.Write(payload); err != nil {
		return err
	}

	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	_ = conn.SetDeadline(time.Now().Add(timeout))

	if _, err := conn.Write(out.Bytes()); err != nil {
		return err
	}

	// read response length
	var respLen int32
	if err := binary.Read(conn, binary.BigEndian, &respLen); err != nil {
		return err
	}
	if respLen < 4 {
		return fmt.Errorf("invalid response length %d", respLen)
	}

	resp := make([]byte, respLen)
	if _, err := io.ReadFull(conn, resp); err != nil {
		return err
	}

	// correlation id is the first 4 bytes of the response
	respCorr := int32(binary.BigEndian.Uint32(resp[0:4]))
	if respCorr != correlationId {
		return fmt.Errorf("correlation id mismatch: got %d expected %d", respCorr, correlationId)
	}
	// success
	return nil
}

// Reachable returns the list of brokers that were reachable during the last check.
func (p *Producer) Reachable() []string {
	if p == nil {
		return nil
	}
	p.reachableMu.RLock()
	defer p.reachableMu.RUnlock()
	out := make([]string, len(p.reachable))
	copy(out, p.reachable)
	return out
}

// IsConnected reports whether at least one broker was reachable.
func (p *Producer) IsConnected() bool {
	if p == nil {
		return false
	}
	p.reachableMu.RLock()
	defer p.reachableMu.RUnlock()
	return len(p.reachable) > 0
}

// Publish is a placeholder for future implementation. It will be implemented
// to speak the Kafka protocol from scratch in later steps of the MVP.
func (p *Producer) Publish(key, value []byte) error {
	// reference parameters to avoid unused parameter warnings until implemented
	_ = key
	_ = value
	return fmt.Errorf("Publish not implemented: producer currently only verifies broker connectivity")
}

// Close is a no-op for the current minimal implementation.
func (p *Producer) Close() error { return nil }
