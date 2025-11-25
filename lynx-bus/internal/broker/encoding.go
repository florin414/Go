package broker

import (
	"bytes"
	"hash/crc32"
	"time"
)

// Protocol-level constants to avoid magic numbers in the encoder.
const (
	produceRequiredAcks int16 = 1
	topicsArraySize     int32 = 1
	partitionsArraySize int32 = 1
	partitionID         int32 = 0
	messageOffset       int64 = 0
	messageMagic        int8  = 0
	messageAttributes   int8  = 0
)

// ProduceConfig centralizes values previously hardcoded inside buildProduceRequest.
type ProduceConfig struct {
	ApiKey        int16 // Produce api key (0)
	ApiVersion    int16 // Produce api version (0)
	CorrelationID int32
	ClientID      string
}

func DefaultProduceConfig() ProduceConfig {
	return ProduceConfig{
		ApiKey:        0,
		ApiVersion:    0,
		CorrelationID: 1,
		ClientID:      "lynx bus",
	}
}

// buildProduceRequest creates a ProduceRequest v0 (framed with int32 length)
// containing a single message for the given topic/partition 0.
// Wire layout (payload) for ProduceRequest v0:
//
//	RequiredAcks(int16) | Timeout(int32) | TopicsArrayCount(int32) |
//	[ TopicName(kafka-string) | PartitionsArrayCount(int32) |
//	  [ Partition(int32) | MessageSet(bytes) ] ]
//
// The final request on the wire is: RequestHeader(apiKey(int16)|apiVersion(int16)|correlationId(int32)|clientId) | payload
func buildProduceRequest(topic string, key, value []byte, timeout time.Duration) ([]byte, error) {
	cfg := DefaultProduceConfig()
	return buildProduceRequestWithConfig(cfg, topic, key, value, timeout)
}

// internal implementation that accepts a config (kept separate for clarity).
func buildProduceRequestWithConfig(cfg ProduceConfig, topic string, key, value []byte, timeout time.Duration) ([]byte, error) {
	// Body buffer contains the ProduceRequest body (excluding api header)
	bodyBuf := &bytes.Buffer{}

	// RequiredAcks (int16)
	if err := writeInt16(bodyBuf, produceRequiredAcks); err != nil {
		return nil, err
	}
	// Timeout (int32) - milliseconds
	if err := writeInt32(bodyBuf, int32(timeout.Milliseconds())); err != nil {
		return nil, err
	}

	// topics array (int32 length = 1)
	if err := writeInt32(bodyBuf, topicsArraySize); err != nil {
		return nil, err
	}
	if err := writeKafkaString(bodyBuf, topic); err != nil {
		return nil, err
	}
	// partitions array (int32 length = 1)
	if err := writeInt32(bodyBuf, partitionsArraySize); err != nil {
		return nil, err
	}
	// partition id (int32) - 0
	if err := writeInt32(bodyBuf, partitionID); err != nil {
		return nil, err
	}

	// build message set bytes and write as Bytes (int32 length + bytes)
	msgSet, err := buildMessageSetBytes(key, value)
	if err != nil {
		return nil, err
	}
	if err := writeBytesWithInt32Len(bodyBuf, msgSet); err != nil {
		return nil, err
	}

	// assemble header + body (header: apiKey(int16) + apiVersion(int16) + correlationId(int32) + clientID)
	reqBuf := &bytes.Buffer{}
	if err := writeInt16(reqBuf, cfg.ApiKey); err != nil {
		return nil, err
	}
	if err := writeInt16(reqBuf, cfg.ApiVersion); err != nil {
		return nil, err
	}
	if err := writeInt32(reqBuf, cfg.CorrelationID); err != nil {
		return nil, err
	}
	// clientID as Kafka string
	if err := writeKafkaString(reqBuf, cfg.ClientID); err != nil {
		return nil, err
	}
	if _, err := reqBuf.Write(bodyBuf.Bytes()); err != nil {
		return nil, err
	}

	// prepend length (total request length)
	full := &bytes.Buffer{}
	if err := writeInt32(full, int32(reqBuf.Len())); err != nil {
		return nil, err
	}
	if _, err := full.Write(reqBuf.Bytes()); err != nil {
		return nil, err
	}
	return full.Bytes(), nil
}

// buildMessageSetBytes creates the message set for a single message (offset 0)
// Format: offset(int64) + messageSize(int32) + message
// Message = CRC(uint32) + magic(int8) + attributes(int8) + key(bytes nullable int32) + value(bytes nullable int32)
func buildMessageSetBytes(key, value []byte) ([]byte, error) {
	payloadBuf := &bytes.Buffer{}
	// magic (int8)
	if err := writeInt8(payloadBuf, messageMagic); err != nil {
		return nil, err
	}
	// attributes (int8)
	if err := writeInt8(payloadBuf, messageAttributes); err != nil {
		return nil, err
	}
	// key (nullable: -1 means null)
	if err := writeNullableBytesWithInt32Len(payloadBuf, key); err != nil {
		return nil, err
	}
	// value (nullable)
	if err := writeNullableBytesWithInt32Len(payloadBuf, value); err != nil {
		return nil, err
	}

	payloadBytes := payloadBuf.Bytes()
	crc := crc32.ChecksumIEEE(payloadBytes)

	messageBuf := &bytes.Buffer{}
	// CRC (uint32)
	if err := writeUint32(messageBuf, crc); err != nil {
		return nil, err
	}
	if _, err := messageBuf.Write(payloadBytes); err != nil {
		return nil, err
	}

	msgSetBuf := &bytes.Buffer{}
	// offset (int64)
	if err := writeInt64(msgSetBuf, messageOffset); err != nil {
		return nil, err
	}
	msgBytes := messageBuf.Bytes()
	// messageSize (int32)
	if err := writeInt32(msgSetBuf, int32(len(msgBytes))); err != nil {
		return nil, err
	}
	if _, err := msgSetBuf.Write(msgBytes); err != nil {
		return nil, err
	}
	return msgSetBuf.Bytes(), nil
}
