package broker

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"time"
)

type Connection interface {
	io.Reader
	io.Writer
	SetDeadline(time.Time) error
	Close() error
}

type Dialer interface {
	DialTimeout(network, addr string, timeout time.Duration) (Connection, error)
}

type NetDialer struct{}

var DefaultDialer Dialer = NetDialer{}

// ------------------------------------------------------
// Config: avoid hardcoded literals in the code
// ------------------------------------------------------

type ApiVersionsConfig struct {
	Network         string
	ClientID        string
	CorrelationID   int32
	ApiKey          int16
	ApiVersion      int16
	MinResponseSize int32
	Dialer          Dialer
}

func DefaultApiVersionsConfig() ApiVersionsConfig {
	return ApiVersionsConfig{
		Network:         "tcp",
		ClientID:        "lynx bus",
		CorrelationID:   1,
		ApiKey:          18,
		ApiVersion:      0,
		MinResponseSize: 4,
		Dialer:          DefaultDialer,
	}
}

// ------------------------------------------------------
// Main logic
// ------------------------------------------------------

// checkApiVersions verifies that the remote address speaks the Kafka API
// by sending an ApiVersionsRequest and verifying the correlation id in the reply.
// This function keeps the original signature (addr, timeout) to avoid breaking
// callers; it uses default config values. If callers want custom behaviour they
// can construct their own request using the helpers in this package.
func checkApiVersions(addr string, timeout time.Duration) error {
	cfg := DefaultApiVersionsConfig()
	return checkApiVersionsWithConfig(addr, timeout, cfg)
}

// checkApiVersionsWithConfig is the configurable implementation that uses the
// provided ApiVersionsConfig.
func checkApiVersionsWithConfig(addr string, timeout time.Duration, cfg ApiVersionsConfig) error {
	req, err := buildApiVersionsRequest(cfg)
	if err != nil {
		return err
	}

	d := cfg.Dialer
	if d == nil {
		d = DefaultDialer
	}

	conn, err := d.DialTimeout(cfg.Network, addr, timeout)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	_ = conn.SetDeadline(time.Now().Add(timeout))

	if err := writeRequest(conn, req); err != nil {
		return err
	}

	resp, err := readResponse(conn, cfg.MinResponseSize)
	if err != nil {
		return err
	}

	respCorr, err := parseCorrelation(resp)
	if err != nil {
		return err
	}
	if respCorr != cfg.CorrelationID {
		return fmt.Errorf("correlation id mismatch: got %d expected %d", respCorr, cfg.CorrelationID)
	}

	return nil
}

func (d NetDialer) DialTimeout(network, addr string, timeout time.Duration) (Connection, error) {
	c, err := net.DialTimeout(network, addr, timeout)
	if err != nil {
		return nil, err
	}
	return c, nil
}

// ------------------------------------------------------
// Request builder (explicit, documented binary layout)
// ------------------------------------------------------

// buildApiVersionsRequest builds the framed request used to probe the broker.
// The wire layout (payload) is: apiKey(int16) | apiVersion(int16) | correlationId(int32) | clientIdLength(int16) | clientId(bytes)
// The final message sent on the wire is: payloadLength(int32) | payload
func buildApiVersionsRequest(cfg ApiVersionsConfig) ([]byte, error) {
	// small buffer for payload fields (without the 4-byte length prefix)
	payloadBuf := &bytes.Buffer{}

	// write apiKey (int16, 2 bytes)
	if err := writeInt16(payloadBuf, cfg.ApiKey); err != nil {
		return nil, err
	}

	// write apiVersion (int16, 2 bytes)
	if err := writeInt16(payloadBuf, cfg.ApiVersion); err != nil {
		return nil, err
	}

	// write correlationId (int32, 4 bytes)
	if err := writeInt32(payloadBuf, cfg.CorrelationID); err != nil {
		return nil, err
	}

	// write clientId as: int16 length + bytes
	if err := writeStringWithInt16Len(payloadBuf, cfg.ClientID); err != nil {
		return nil, err
	}

	payload := payloadBuf.Bytes()

	// now prefix with the 4-byte payload length
	out := &bytes.Buffer{}
	if err := writeInt32(out, int32(len(payload))); err != nil {
		return nil, err
	}
	if _, err := out.Write(payload); err != nil {
		return nil, err
	}

	return out.Bytes(), nil
}

func writeRequest(c Connection, data []byte) error {
	_, err := c.Write(data)
	return err
}

// readResponse now uses shared readInt32 from binary_helpers.go
func readResponse(c Connection, minSize int32) ([]byte, error) {
	// read 4-byte length using shared helper (explicit about size and endianness)
	respLen, err := readInt32(c)
	if err != nil {
		return nil, err
	}
	if respLen < minSize {
		return nil, fmt.Errorf("invalid response length %d", respLen)
	}

	resp := make([]byte, respLen)
	if _, err := io.ReadFull(c, resp); err != nil {
		return nil, err
	}

	return resp, nil
}

// parseCorrelation delegates to shared helper for clarity
func parseCorrelation(resp []byte) (int32, error) {
	return parseCorrelationFromBytes(resp)
}
