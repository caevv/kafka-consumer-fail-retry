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

	wg.Add(2)
	go func() {
		consumeHandler := func() error {
			calledTimes++
			log.Printf("called times: %v", calledTimes)
			if calledTimes%2 == 0 {
				return errors.New("something went wrong")
			}
			wg.Done()
			return nil
		}

		for {
			for attempt := 1; attempt <= maxAttempts; attempt++ {
				log.Printf("attempt: %v", attempt)
				err = consumer.consume(consumeHandler)
				if err != nil {
					log.Printf("failed to consume: %v", err)
					err = consumer.close()
					if err != nil {
						log.Printf("failed to close consumer: %v", err)
					}
					time.Sleep(backoffDuration)
					consumer, err = newConsumer(broker, group, topic)
					if err != nil {
						log.Fatalf("failed to create new consumer: %v", err)
					}
				} else {
					break
				}
			}
		}
	}()

	time.Sleep(15 * time.Second)

	producer, err := newProducer("kafka-broker:9092")
	if err != nil {
		t.Fatalf("failed to create new producer: %v", err)
	}

	messages := [][]byte{
		[]byte("test-message 1"),
		[]byte("test-message 2"),
	}

	for _, message := range messages {
		err = producer.produce("test-topic", message)
		if err != nil {
			t.Fatalf("failed to produce message: %v", err)
		}
	}

	waitTimeout(&wg, 30*time.Second)

	expected := 3

	if expected != calledTimes {
		t.Fatalf("expected: %v called times, got: %v", expected, calledTimes)
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
