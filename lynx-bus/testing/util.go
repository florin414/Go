package testing

import (
	"net"
	"testing"
)

// GetFreeAddr obtains a free TCP address by binding to :0 and closing immediately.
func GetFreeAddr(t *testing.T) string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to get free addr: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}
