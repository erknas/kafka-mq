package main

import (
	"fmt"
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type OrderProducer struct {
	producer   *kafka.Producer
	topic      string
	deliverych chan kafka.Event
}

func NewOrderProducer(p *kafka.Producer, topic string) *OrderProducer {
	return &OrderProducer{
		producer:   p,
		topic:      topic,
		deliverych: make(chan kafka.Event),
	}
}

func (op *OrderProducer) produceOrder(orderType string, queueNum int) error {
	var (
		format  = fmt.Sprintf("%s - %d", orderType, queueNum)
		payload = []byte(format)
	)

	err := op.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &op.topic, Partition: kafka.PartitionAny},
		Value:          payload,
	},
		op.deliverych,
	)
	if err != nil {
		return err
	}

	<-op.deliverych

	return nil
}

func main() {
	topic := "topicName"

	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"client.id":         "myProducer",
		"acks":              "all"})

	if err != nil {
		log.Fatalf("Failed to create producer: %s\n", err)
	}

	go func() {
		consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
			"bootstrap.servers": "localhost:9092",
			"group.id":          "myConsumer",
			"auto.offset.reset": "smallest"})
		if err != nil {
			log.Fatal(err)
		}

		if err = consumer.Subscribe(topic, nil); err != nil {
			log.Fatal(err)
		}

		for {
			ev := consumer.Poll(100)
			switch e := ev.(type) {
			case *kafka.Message:
				fmt.Printf("consumed message: %s\n", e.Value)
			case *kafka.Error:
				fmt.Printf("%v\n", e)
			}
		}
	}()

	op := NewOrderProducer(p, topic)
	for i := 1; i <= 100; i++ {
		if err := op.produceOrder("some msg", i); err != nil {
			log.Fatal(err)
		}
		time.Sleep(time.Second * 2)
	}
}
