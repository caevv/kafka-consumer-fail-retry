package kafka

import (
	"log"
	"sync"
	"testing"
	"time"

	"github.com/pkg/errors"
)

func TestConsumer_Fail_Retry(t *testing.T) {
	var (
		maxAttempts  = 2
		retryEnabled = true
	)

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

	var (
		calledTimes = 0
		wg          sync.WaitGroup
	)

	wg.Add(maxAttempts)
	go func() {
		err = consumer.consume(func() error {
			defer wg.Done()
			calledTimes++
			log.Printf("called times: %v", calledTimes)
			return errors.New("something went wrong")
		}, 1)
		if err != nil {
			log.Printf("failed to consume: %v", err)
		}
	}()

	time.Sleep(15 * time.Second)

	producer, err := newProducer("kafka-broker:9092")
	if err != nil {
		t.Fatalf("failed to create new producer: %v", err)
	}

	err = producer.produce("test-topic", []byte("test-message"))
	if err != nil {
		t.Fatalf("failed to produce message: %v", err)
	}

	waitTimeout(&wg, 30*time.Second)

	if calledTimes != maxAttempts {
		t.Fatalf("expected %v attempts, got: %v", maxAttempts, calledTimes)
	}
}

func waitTimeout(wg *sync.WaitGroup, timeout time.Duration) bool {
	c := make(chan struct{})
	go func() {
		defer close(c)
		wg.Wait()
	}()
	select {
	case <-c:
		return false // completed normally
	case <-time.After(timeout):
		return true // timed out
	}
}
