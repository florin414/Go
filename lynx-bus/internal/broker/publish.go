package broker

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"time"
)

// Publish sends a single message to the first reachable broker. It delegates
// message encoding and response parsing to helper files to keep this file small.
func (p *Producer) Publish(key, value []byte) error {
	if p == nil {
		return fmt.Errorf("producer is nil")
	}
	if len(value) == 0 {
		return fmt.Errorf("value required")
	}

	p.reachableMu.RLock()
	brokers := make([]string, len(p.reachable))
	copy(brokers, p.reachable)
	p.reachableMu.RUnlock()

	if len(brokers) == 0 {
		return fmt.Errorf("no reachable brokers")
	}
	topic := p.cfg.Topic
	if topic == "" {
		return fmt.Errorf("no topic configured")
	}

	addr := chooseBroker(brokers)
	timeout := p.cfg.DialTimeout
	if timeout == 0 {
		timeout = 5 * time.Second
	}

	// build request bytes
	reqBytes, err := buildProduceRequest(topic, key, value, timeout)
	if err != nil {
		return fmt.Errorf("build produce request: %w", err)
	}

	// dial and send
	conn, err := dialBroker(addr, timeout)
	if err != nil {
		return fmt.Errorf("dial broker %s: %w", addr, err)
	}
	defer func() { _ = conn.Close() }()

	respBytes, err := sendRequestAndReadResponse(conn, reqBytes, timeout)
	if err != nil {
		return fmt.Errorf("send/receive produce: %w", err)
	}

	// parse response
	if err := parseProduceResponse(respBytes); err != nil {
		return err
	}
	return nil
}

// chooseBroker currently returns the first broker; kept as a function to
// make behaviour explicit and easy to change later.
func chooseBroker(brokers []string) string {
	if len(brokers) == 0 {
		return ""
	}
	return brokers[0]
}

// dialBroker opens a TCP connection with timeout.
func dialBroker(addr string, timeout time.Duration) (net.Conn, error) {
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return nil, err
	}
	_ = conn.SetDeadline(time.Now().Add(timeout))
	return conn, nil
}

// sendRequestAndReadResponse writes the full framed request and reads the framed response bytes.
func sendRequestAndReadResponse(conn net.Conn, req []byte, timeout time.Duration) ([]byte, error) {
	_ = conn.SetDeadline(time.Now().Add(timeout))
	if _, err := conn.Write(req); err != nil {
		return nil, fmt.Errorf("write produce request: %w", err)
	}
	var respLen int32
	if err := binary.Read(conn, binary.BigEndian, &respLen); err != nil {
		return nil, fmt.Errorf("read response len: %w", err)
	}
	if respLen <= 0 {
		return nil, fmt.Errorf("invalid response length %d", respLen)
	}
	resp := make([]byte, respLen)
	if _, err := io.ReadFull(conn, resp); err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}
	return resp, nil
}

// parseProduceResponse parses a ProduceResponse v0 from the provided response
// bytes (without the length prefix). It returns an error if any partition
// reports a non-zero error code or if the response is malformed.
func parseProduceResponse(resp []byte) error {
	r := bytes.NewReader(resp)
	var respCorr int32
	if err := binary.Read(r, binary.BigEndian, &respCorr); err != nil {
		return fmt.Errorf("parse response correlation: %w", err)
	}
	// correlation id is expected to be 1 for our simple implementation
	if respCorr != 1 {
		return fmt.Errorf("correlation id mismatch: got %d expected %d", respCorr, 1)
	}
	var topicsCount int32
	if err := binary.Read(r, binary.BigEndian, &topicsCount); err != nil {
		return fmt.Errorf("parse response topics count: %w", err)
	}
	if topicsCount == 0 {
		return fmt.Errorf("no topics in produce response")
	}
	for i := int32(0); i < topicsCount; i++ {
		tName, err := readKafkaString(r)
		if err != nil {
			return fmt.Errorf("read topic name: %w", err)
		}
		var partCount int32
		if err := binary.Read(r, binary.BigEndian, &partCount); err != nil {
			return fmt.Errorf("read partition count: %w", err)
		}
		for j := int32(0); j < partCount; j++ {
			var partition int32
			if err := binary.Read(r, binary.BigEndian, &partition); err != nil {
				return fmt.Errorf("read partition: %w", err)
			}
			var errCode int16
			if err := binary.Read(r, binary.BigEndian, &errCode); err != nil {
				return fmt.Errorf("read errorCode: %w", err)
			}
			var offset int64
			if err := binary.Read(r, binary.BigEndian, &offset); err != nil {
				return fmt.Errorf("read offset: %w", err)
			}
			if errCode != 0 {
				return fmt.Errorf("produce error for topic %s partition %d: code %d", tName, partition, errCode)
			}
		}
	}
	return nil
}
