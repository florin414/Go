package testing

import (
	"time"

	"github.com/florin/lynx-bus/internal/broker"
)

// NewProducerConfig constructs a ProducerConfig for tests using provided values.
func NewProducerConfig(brokers []string, topic string, dialTimeout time.Duration) broker.ProducerConfig {
	return broker.ProducerConfig{
		Brokers:     brokers,
		Topic:       topic,
		DialTimeout: dialTimeout,
	}
}
