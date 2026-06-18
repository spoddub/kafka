package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type User struct {
	Name           string `json:"name"`
	FavoriteNumber int64  `json:"favorite_number"`
	FavoriteColor  string `json:"favorite_color"`
}

func main() {
	const (
		bootstrapServers = "localhost:9093"
		caLocation       = "certs/ca/ca.crt"
	)

	topic := "test"

	config := &kafka.ConfigMap{
		"bootstrap.servers":     bootstrapServers,
		"security.protocol":     "SSL",
		"ssl.ca.location":       caLocation,
		"broker.address.family": "v4",
	}

	producer, err := kafka.NewProducer(config)
	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
		os.Exit(1)
	}
	defer producer.Close()

	value := User{
		Name:           "First user",
		FavoriteNumber: 42,
		FavoriteColor:  "blue",
	}

	payload, err := json.Marshal(value)
	if err != nil {
		fmt.Printf("Failed to serialize payload: %s\n", err)
		os.Exit(1)
	}

	deliveryChan := make(chan kafka.Event, 1)
	defer close(deliveryChan)

	err = producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Value: payload,
		Headers: []kafka.Header{
			{
				Key:   "myTestHeader",
				Value: []byte("header values are binary"),
			},
		},
	}, deliveryChan)
	if err != nil {
		fmt.Printf("Produce failed: %v\n", err)
		os.Exit(1)
	}

	event := <-deliveryChan

	message, ok := event.(*kafka.Message)
	if !ok {
		fmt.Printf("Unexpected delivery event: %v\n", event)
		os.Exit(1)
	}

	if message.TopicPartition.Error != nil {
		fmt.Printf("Delivery failed: %v\n", message.TopicPartition.Error)
		os.Exit(1)
	}

	fmt.Printf(
		"Delivered message to topic %s [%d] at offset %v\n",
		*message.TopicPartition.Topic,
		message.TopicPartition.Partition,
		message.TopicPartition.Offset,
	)
}
