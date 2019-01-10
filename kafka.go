package kafka

import (
	"log"
	"time"

	confluentkafka "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/davecgh/go-spew/spew"
	"github.com/pkg/errors"
)

type (
	consumer struct {
		consumer        *confluentkafka.Consumer
		topic           string
		retryEnabled    bool
		maxAttempts     int
		backoffDuration time.Duration
	}
)

func newConsumer(broker, group, topic string, retryEnabled bool, maxAttempts int, backoffDuration time.Duration) (*consumer, error) {
	conf := confluentkafka.ConfigMap{
		"bootstrap.servers":    broker,
		"group.id":             group,
		"session.timeout.ms":   6000,
		"default.topic.config": confluentkafka.ConfigMap{"auto.offset.reset": "latest"},
		"enable.auto.commit":   false,
		"debug":                "consumer",
	}

	kafkaConsumer, err := confluentkafka.NewConsumer(&conf)
	if err != nil {
		return nil, errors.Wrap(err, "failed to connect to kafka broker")
	}

	return &consumer{
		consumer:        kafkaConsumer,
		topic:           topic,
		retryEnabled:    retryEnabled,
		maxAttempts:     maxAttempts,
		backoffDuration: backoffDuration,
	}, nil
}

func (c consumer) consume(handler func() error, attempt int) error {
	var err error

	defer func() {
		if err == nil {
			return
		}

		log.Printf("error while consuming message: %v", err)

		if c.retryEnabled && attempt <= c.maxAttempts {
			time.Sleep(c.backoffDuration)
			err = c.consume(handler, attempt+1)
		}
	}()

	err = c.consumer.SubscribeTopics([]string{c.topic}, nil)
	if err != nil {
		return errors.Wrap(err, "failed to subscribe to topic")
	}

	log.Print("listening")

	for {
		event := c.consumer.Poll(100)
		if event == nil {
			continue
		}

		switch eventType := event.(type) {
		case *confluentkafka.Message:
			// deserialize message

			err = handler()
			if err != nil {
				return errors.Wrap(err, "failed to handle message")
			}

			// commit
		case confluentkafka.PartitionEOF:
			return errors.New("reached end of queue")
		case confluentkafka.Error:
			return eventType
		default:
			log.Printf("ignored: %v", spew.Sdump(eventType))
		}
	}
}

type producer struct {
	producer *confluentkafka.Producer
}

func newProducer(broker string) (*producer, error) {
	conf := confluentkafka.ConfigMap{
		"bootstrap.servers": broker,
	}

	kafkaProducer, err := confluentkafka.NewProducer(&conf)
	if err != nil {
		return nil, errors.Wrap(err, "failed to connect to producer broker")
	}

	return &producer{producer: kafkaProducer}, nil
}

func (producer producer) produce(topic string, message []byte) error {
	deliveryChan := make(chan confluentkafka.Event)

	err := producer.producer.Produce(
		&confluentkafka.Message{
			TopicPartition: confluentkafka.TopicPartition{Topic: &topic, Partition: confluentkafka.PartitionAny},
			Value:          message,
		},
		deliveryChan,
	)
	if err != nil {
		return errors.Wrap(err, "message could not be enqueued")
	}

	event := <-deliveryChan
	m := event.(*confluentkafka.Message)

	if m.TopicPartition.Error != nil {
		return errors.Wrap(m.TopicPartition.Error, "delivery failed")
	}

	close(deliveryChan)

	return nil
}
