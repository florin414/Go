package testing

import (
	"bytes"
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
		defer func() { _ = ln.Close() }()
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
				defer func() { _ = c.Close() }()
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
		defer func() { _ = ln.Close() }()
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
				defer func() { _ = c.Close() }()
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

// StartKafkaMock_Produce_Success starts a mock that responds to ApiVersions requests
// (apiKey=18) like StartKafkaMock_Valid and responds to Produce requests (apiKey=0)
// with a successful ProduceResponse (errorCode=0).
func StartKafkaMock_Produce_Success(t *testing.T) (addr string, closeFn func()) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to start produce success mock: %v", err)
	}
	addr = ln.Addr().String()
	stop := make(chan struct{})
	go func() {
		defer func() { _ = ln.Close() }()
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
				defer func() { _ = c.Close() }()
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
				// apiKey is first 2 bytes
				apiKey := int16(binary.BigEndian.Uint16(payload[0:2]))
				corr := payload[4:8]
				if apiKey == 18 { // ApiVersionsRequest
					// echo correlation id like valid mock
					resp := make([]byte, 4+4)
					binary.BigEndian.PutUint32(resp[0:4], uint32(4))
					copy(resp[4:8], corr)
					_, _ = c.Write(resp)
					return
				}
				if apiKey == 0 { // ProduceRequest => respond with ProduceResponse v0
					// Build response payload: correlationId(int32) | topicsCount(int32)
					// [ topic(string) | partitionsCount(int32) | [ partition(int32) | errorCode(int16) | offset(int64) ] ]
					buf := &bytes.Buffer{}
					// correlation id
					buf.Write(corr)
					// topicsCount = 1
					_ = binary.Write(buf, binary.BigEndian, int32(1))
					// topic name as int16 len + bytes; reuse topic 't'
					_ = binary.Write(buf, binary.BigEndian, int16(1))
					buf.Write([]byte("t"))
					// partitionsCount = 1
					_ = binary.Write(buf, binary.BigEndian, int32(1))
					// partition = 0
					_ = binary.Write(buf, binary.BigEndian, int32(0))
					// errorCode = 0
					_ = binary.Write(buf, binary.BigEndian, int16(0))
					// offset = 123
					_ = binary.Write(buf, binary.BigEndian, int64(123))
					payloadOut := buf.Bytes()
					// prefix length
					resp := make([]byte, 4+len(payloadOut))
					binary.BigEndian.PutUint32(resp[0:4], uint32(len(payloadOut)))
					copy(resp[4:], payloadOut)
					_, _ = c.Write(resp)
					return
				}
				// unknown apiKey -> do nothing
			}(conn)
		}
	}()
	return addr, func() { close(stop); _ = ln.Close() }
}

// StartKafkaMock_Produce_Error starts a mock that replies to ProduceRequest with a non-zero error code.
func StartKafkaMock_Produce_Error(t *testing.T, errCode int16) (addr string, closeFn func()) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to start produce error mock: %v", err)
	}
	addr = ln.Addr().String()
	stop := make(chan struct{})
	go func() {
		defer func() { _ = ln.Close() }()
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
				defer func() { _ = c.Close() }()
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
				apiKey := int16(binary.BigEndian.Uint16(payload[0:2]))
				corr := payload[4:8]
				if apiKey == 18 { // ApiVersionsRequest
					resp := make([]byte, 4+4)
					binary.BigEndian.PutUint32(resp[0:4], uint32(4))
					copy(resp[4:8], corr)
					_, _ = c.Write(resp)
					return
				}
				if apiKey == 0 { // ProduceRequest => respond with ProduceResponse v0 with error
					buf := &bytes.Buffer{}
					buf.Write(corr)
					_ = binary.Write(buf, binary.BigEndian, int32(1))
					_ = binary.Write(buf, binary.BigEndian, int16(1))
					buf.Write([]byte("t"))
					_ = binary.Write(buf, binary.BigEndian, int32(1))
					_ = binary.Write(buf, binary.BigEndian, int32(0))
					_ = binary.Write(buf, binary.BigEndian, errCode)
					_ = binary.Write(buf, binary.BigEndian, int64(0))
					payloadOut := buf.Bytes()
					resp := make([]byte, 4+len(payloadOut))
					binary.BigEndian.PutUint32(resp[0:4], uint32(len(payloadOut)))
					copy(resp[4:], payloadOut)
					_, _ = c.Write(resp)
					return
				}
			}(conn)
		}
	}()
	return addr, func() { close(stop); _ = ln.Close() }
}
