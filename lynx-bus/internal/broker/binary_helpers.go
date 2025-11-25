package broker

import (
	"encoding/binary"
	"fmt"
	"io"
)

// Shared binary helpers used by multiple encoder modules.
// Make endianness and field sizes explicit and avoid duplication.

const (
	maxKafkaStringLenShared = 0x7fff
)

func writeInt8(w io.Writer, v int8) error {
	b := []byte{byte(v)}
	_, err := w.Write(b)
	return err
}

func writeInt16(w io.Writer, v int16) error {
	var tmp [2]byte
	binary.BigEndian.PutUint16(tmp[:], uint16(v))
	_, err := w.Write(tmp[:])
	return err
}

func writeInt32(w io.Writer, v int32) error {
	var tmp [4]byte
	binary.BigEndian.PutUint32(tmp[:], uint32(v))
	_, err := w.Write(tmp[:])
	return err
}

func writeUint32(w io.Writer, v uint32) error {
	var tmp [4]byte
	binary.BigEndian.PutUint32(tmp[:], v)
	_, err := w.Write(tmp[:])
	return err
}

func writeInt64(w io.Writer, v int64) error {
	var tmp [8]byte
	binary.BigEndian.PutUint64(tmp[:], uint64(v))
	_, err := w.Write(tmp[:])
	return err
}

func writeBytesWithInt32Len(w io.Writer, b []byte) error {
	if err := writeInt32(w, int32(len(b))); err != nil {
		return err
	}
	_, err := w.Write(b)
	return err
}

// writeKafkaString writes a Kafka string (int16 length + bytes)
func writeKafkaString(w io.Writer, s string) error {
	if len(s) > maxKafkaStringLenShared {
		return fmt.Errorf("string too long")
	}
	if err := writeInt16(w, int16(len(s))); err != nil {
		return err
	}
	_, err := w.Write([]byte(s))
	return err
}

// writeStringWithInt16Len is an alias used by some callers; kept for clarity.
func writeStringWithInt16Len(w io.Writer, s string) error {
	return writeKafkaString(w, s)
}

// writeNullableBytesWithInt32Len writes a nullable byte array prefixed by int32 length
// When b==nil it writes -1 as int32 (Kafka's nullable bytes convention)
func writeNullableBytesWithInt32Len(w io.Writer, b []byte) error {
	if b == nil {
		return writeInt32(w, int32(-1))
	}
	return writeBytesWithInt32Len(w, b)
}

// readInt16 reads a big-endian int16 from r
func readInt16(r io.Reader) (int16, error) {
	var tmp [2]byte
	if _, err := io.ReadFull(r, tmp[:]); err != nil {
		return 0, err
	}
	return int16(binary.BigEndian.Uint16(tmp[:])), nil
}

// readInt32 reads a big-endian int32 from r
func readInt32(r io.Reader) (int32, error) {
	var tmp [4]byte
	if _, err := io.ReadFull(r, tmp[:]); err != nil {
		return 0, err
	}
	return int32(binary.BigEndian.Uint32(tmp[:])), nil
}

// parseCorrelationFromBytes extracts the int32 correlation id from the first
// 4 bytes of payload (big-endian). Returns error if payload too short.
func parseCorrelationFromBytes(resp []byte) (int32, error) {
	if len(resp) < 4 {
		return 0, fmt.Errorf("response too short")
	}
	return int32(binary.BigEndian.Uint32(resp[:4])), nil
}

// readKafkaString reads a Kafka string (int16 length + bytes). A negative length
// is interpreted as a nil/empty string per Kafka protocol conventions.
func readKafkaString(r io.Reader) (string, error) {
	l, err := readInt16(r)
	if err != nil {
		return "", err
	}
	if l < 0 {
		// nullable string: treat as empty
		return "", nil
	}
	buf := make([]byte, l)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}
	return string(buf), nil
}
