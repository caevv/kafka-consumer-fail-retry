package kafka

import (
	"log"

	confluentkafka "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/davecgh/go-spew/spew"
	"github.com/pkg/errors"
)

type consumer struct {
	consumer *confluentkafka.Consumer
	topic    string
}

func newConsumer(broker, group, topic string) (*consumer, error) {
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
		consumer: kafkaConsumer,
		topic:    topic,
	}, nil
}

func (c consumer) consume(handler func() error) error {
	var err error

	defer func() {
		if err == nil {
			return
		}

		log.Printf("error while consuming message: %v", err)
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
			log.Println("deserialize message")

			log.Println("handle message")
			err = handler()
			if err != nil {
				return errors.Wrap(err, "failed to handle message")
			}

			log.Println("commit offset")
			_, err := c.consumer.Commit()
			if err != nil {
				return errors.Wrap(err, "failed to commit")
			}
		case confluentkafka.PartitionEOF:
			log.Println("reached end of queue")
		case confluentkafka.Error:
			return eventType
		default:
			log.Printf("ignored: %v", spew.Sdump(eventType))
		}
	}
}

func (c consumer) close() error {
	return c.consumer.Close()
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

	log.Printf("message inserted on queue: %v. message key: %v, message: %v, topic: %v, offset: %v, partition: %v, timestamp: %v",
		topic,
		string(m.Key),
		spew.Sdump(m.Value),
		*m.TopicPartition.Topic,
		m.TopicPartition.Offset,
		m.TopicPartition.Partition,
		m.Timestamp,
	)

	close(deliveryChan)

	return nil
}
