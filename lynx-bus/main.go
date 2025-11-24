package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"LynxBus/internal/broker"
)

func main() {
	// start a mock broker for local development so the producer can connect
	mockAddr := "localhost:9092"
	ready := make(chan struct{})
	go startMockBroker(mockAddr, ready)
	// wait up to 2s for mock to be ready
	select {
	case <-ready:
	case <-time.After(2 * time.Second):
		log.Fatalf("mock broker failed to start on %s", mockAddr)
	}

	cfg := broker.ProducerConfig{
		Brokers:     []string{mockAddr},
		Topic:       "test-topic",
		DialTimeout: 3 * time.Second,
	}

	p, err := broker.NewProducer(cfg)

	if err != nil {
		log.Fatalf("failed to create producer: %v", err)
	}
	defer func() {
		_ = p.Close()
	}()

	fmt.Printf("connected: %v\n", p.IsConnected())
	reachable := p.Reachable()
	fmt.Printf("reachable brokers: %v\n", reachable)

	// block until interrupt to allow inspection and to stop mock
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	fmt.Println("shutting down")
}

// startMockBroker runs a minimal TCP server that understands the framed
// request format used by checkApiVersions: 4-byte BE length followed by
// payload. It reads the correlation id from the request and responds with
// a framed message whose payload is just the 4-byte correlation id.
func startMockBroker(addr string, ready chan struct{}) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("failed to start mock broker: %v", err)
	}
	defer func() { _ = ln.Close() }()
	close(ready)
	log.Printf("mock broker listening on %s", addr)
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("mock accept error: %v", err)
			continue
		}
		go func(c net.Conn) {
			defer func() { _ = c.Close() }()
			// set a deadline per-connection to avoid hanging
			_ = c.SetDeadline(time.Now().Add(5 * time.Second))
			var lenBuf [4]byte
			if _, err := io.ReadFull(c, lenBuf[:]); err != nil {
				return
			}
			msgLen := int32(binary.BigEndian.Uint32(lenBuf[:]))
			if msgLen <= 0 {
				return
			}
			payload := make([]byte, msgLen)
			if _, err := io.ReadFull(c, payload); err != nil {
				return
			}
			// correlation id is at bytes 4..7 (after apiKey(2) + apiVersion(2))
			if len(payload) < 8 {
				return
			}
			corr := payload[4:8]
			// respond with length=4 and the same correlation id
			resp := make([]byte, 4+4)
			binary.BigEndian.PutUint32(resp[0:4], uint32(4))
			copy(resp[4:8], corr)
			_, _ = c.Write(resp)
		}(conn)
	}
}
