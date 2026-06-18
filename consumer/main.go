package main

import (
	"errors"
	"fmt"
	"os"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {
	const (
		bootstrapServers = "localhost:9093"
		topic            = "test"
		groupID          = "my-consumer-group"
		caLocation       = "certs/ca/ca.crt"
	)

	config := &kafka.ConfigMap{
		"bootstrap.servers":     bootstrapServers,
		"security.protocol":     "SSL",
		"ssl.ca.location":       caLocation,
		"broker.address.family": "v4",
		"group.id":              groupID,
		"auto.offset.reset":     "earliest",
	}

	consumer, err := kafka.NewConsumer(config)
	if err != nil {
		fmt.Printf("Failed to create consumer: %s\n", err)
		os.Exit(1)
	}
	defer consumer.Close()

	err = consumer.SubscribeTopics([]string{topic}, nil)
	if err != nil {
		fmt.Printf("Failed to subscribe to topic %s: %s\n", topic, err)
		os.Exit(1)
	}

	fmt.Printf("Consumer subscribed to topic %s\n", topic)

	for {
		msg, err := consumer.ReadMessage(-1)
		if err != nil {
			var kafkaErr kafka.Error
			ok := errors.As(err, &kafkaErr)
			if ok && kafkaErr.IsFatal() {
				fmt.Printf("Fatal consumer error: %v\n", kafkaErr)
				break
			}

			fmt.Printf("Consumer error: %v\n", err)
			continue
		}

		fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
	}
}
