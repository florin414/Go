package testing

import (
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"
)

// StartKafkaMock_Valid starts a mock that responds with the same correlation id (valid Kafka-like reply).
// Returns the listening address and a closer function.
func StartKafkaMock_Valid(t *testing.T) (addr string, closeFn func()) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to start good mock: %v", err)
	}
	addr = ln.Addr().String()
	stop := make(chan struct{})
	go func() {
		defer ln.Close()
		for {
			select {
			case <-stop:
				return
			default:
			}
			conn, err := ln.Accept()
			if err != nil {
				select {
				case <-stop:
					return
				default:
					continue
				}
			}
			go func(c net.Conn) {
				defer c.Close()
				_ = c.SetDeadline(time.Now().Add(3 * time.Second))
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
				if len(payload) < 8 {
					return
				}
				corr := payload[4:8]
				resp := make([]byte, 4+4)
				binary.BigEndian.PutUint32(resp[0:4], uint32(4))
				copy(resp[4:8], corr)
				_, _ = c.Write(resp)
			}(conn)
		}
	}()
	return addr, func() { close(stop); _ = ln.Close() }
}

// StartKafkaMock_Invalid starts a mock that responds with a wrong correlation id (invalid Kafka reply).
func StartKafkaMock_Invalid(t *testing.T) (addr string, closeFn func()) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to start bad mock: %v", err)
	}
	addr = ln.Addr().String()
	stop := make(chan struct{})
	go func() {
		defer ln.Close()
		for {
			select {
			case <-stop:
				return
			default:
			}
			conn, err := ln.Accept()
			if err != nil {
				select {
				case <-stop:
					return
				default:
					continue
				}
			}
			go func(c net.Conn) {
				defer c.Close()
				_ = c.SetDeadline(time.Now().Add(3 * time.Second))
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
				// reply with a different correlation id (zero)
				resp := make([]byte, 4+4)
				binary.BigEndian.PutUint32(resp[0:4], uint32(4))
				binary.BigEndian.PutUint32(resp[4:8], uint32(0))
				_, _ = c.Write(resp)
			}(conn)
		}
	}()
	return addr, func() { close(stop); _ = ln.Close() }
}
