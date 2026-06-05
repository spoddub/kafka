package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type Item struct {
	ProductID string  `json:"product_id"`
	Quantity  int     `json:"quantity"`
	Price     float64 `json:"price"`
}

type Order struct {
	OrderID    string  `json:"order_id"`
	UserID     string  `json:"user_id"`
	Items      []Item  `json:"items"`
	TotalPrice float64 `json:"total_price"`
}

func main() {
	if len(os.Args) < 3 {
		log.Fatalf("usage: %s <bootstrap_servers> <topic>\n", os.Args[0])
	}

	bootstrapServers := os.Args[1]
	topic := os.Args[2]

	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
	})
	if err != nil {
		log.Fatalf("failed to create producer: %s\n", err)
	}
	defer producer.Close()

	deliveryChan := make(chan kafka.Event)
	defer close(deliveryChan)

	order := Order{
		OrderID: "0001",
		UserID:  "00001",
		Items: []Item{
			{"535", 1, 300},
			{"125", 2, 100},
		},
		TotalPrice: 500.00,
	}

	payload, err := json.Marshal(order)
	if err != nil {
		log.Fatalf("failed to marshal order: %s\n", err)
	}

	err = producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Key:   []byte(order.OrderID),
		Value: payload,
		Headers: []kafka.Header{
			{Key: "content-type", Value: []byte("application/json")},
		},
	}, deliveryChan)
	if err != nil {
		log.Fatalf("failed to produce message: %s\n", err)
	}

	event := <-deliveryChan
	message := event.(*kafka.Message)

	if message.TopicPartition.Error != nil {
		fmt.Printf("delivery failed: %v\n", message.TopicPartition.Error)
	}

	fmt.Printf(
		"Message delivered to topic %s [%d] at offset %v\n",
		*message.TopicPartition.Topic,
		message.TopicPartition.Partition,
		message.TopicPartition.Offset)
}
