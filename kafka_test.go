package kafka

import (
	"log"
	"sync"
	"testing"
	"time"

	"github.com/pkg/errors"
)

const (
	broker = "kafka-broker:9092"
	group  = "test-group"
	topic  = "test-topic"
)

func TestConsumer_Fail_Retry(t *testing.T) {
	var (
		maxAttempts     = 2
		backoffDuration = 1 * time.Second
		calledTimes     = 0
		wg              sync.WaitGroup
	)

	consumer, err := newConsumer(broker, group, topic)
	if err != nil {
		t.Fatalf("failed to create new consumer: %v", err)
	}

	wg.Add(maxAttempts)
	go func() {
		attempt := 1
		handler := func() error {
			defer wg.Done()
			calledTimes++
			log.Printf("called times: %v", calledTimes)
			return errors.New("something went wrong")
		}
		err = consumer.consume(handler, attempt)
		if err != nil {
			log.Printf("failed to consume: %v", err)
			err = consumer.close()
			if err != nil {
				log.Printf("failed to close consumer: %v", err)
			}
			for attempt++; attempt <= maxAttempts; attempt++ {
				time.Sleep(backoffDuration)
				log.Printf("attempt: %v", attempt)
				consumer, err = newConsumer(broker, group, topic)
				err = consumer.consume(handler, attempt)
				if err != nil {
					log.Printf("failed to consume: %v", err)
					err = consumer.close()
					if err != nil {
						log.Printf("failed to close consumer: %v", err)
					}
				}
			}
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
