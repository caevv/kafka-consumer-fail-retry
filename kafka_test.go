package kafka

import (
	"testing"
	"time"

	"github.com/pkg/errors"
)

func TestConsumer_Fail_Retry(t *testing.T) {
	maxAttempts := 2
	retryEnabled := true

	consumer, err := newConsumer(
		"kafka-broker:9092",
		"test-group",
		"test-topic",
		retryEnabled,
		maxAttempts,
		1*time.Second,
	)
	if err != nil {
		t.Fatalf("failed to create new consumer: %v", err)
	}

	producer, err := newProducer("kafka-broker:9092")
	if err != nil {
		t.Fatalf("failed to create new producer: %v", err)
	}

	err = producer.produce("test-topic", []byte("test-message"))
	if err != nil {
		t.Fatalf("failed to produce message: %v", err)
	}

	i := 0
	err = consumer.consume(func() error {
		i += 1
		return errors.New("something went wrong")
	}, 1)
	if err != nil {
		t.Fatalf("failed to consume: %v", err)
	}

	if i != 2 {
		t.Fatalf("expected %v attempts, got: %v", maxAttempts, i)
	}
}
