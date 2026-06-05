package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

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

const timeoutMs = 100

func main() {
	if len(os.Args) != 3 {
		log.Fatalf("usage: %s <bootstrap-servers> <topic>\n", os.Args[0])
	}

	bootstrapServers := os.Args[1]
	topic := os.Args[2]

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
		"group.id":          "consumer_group_1",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		log.Fatalf("failed to create consumer: %v\n", err)
	}
	defer consumer.Close()

	err = consumer.SubscribeTopics([]string{topic}, nil)
	if err != nil {
		log.Fatalf("failed to subscribe to topic: %v\n", err)
	}

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	fmt.Printf("Consumer started. Topic: %s\n", topic)

	run := true

	for run {
		select {
		case sig := <-sigchan:
			fmt.Printf("Got signal %v. Shutting down...\n", sig)
			run = false

		default:
			event := consumer.Poll(timeoutMs)
			if event == nil {
				continue
			}

			switch e := event.(type) {
			case *kafka.Message:
				var order Order

				err := json.Unmarshal(e.Value, &order)
				if err != nil {
					fmt.Printf("failed to unmarshal message: %v\n", err)
					continue
				}

				fmt.Printf("Message on %s\n", e.TopicPartition)
				fmt.Printf("key: %s\n", string(e.Key))
				fmt.Printf("order: %+v\n", order)

				if e.Headers != nil {
					fmt.Printf("headers: %v\n", e.Headers)
				}

			case kafka.Error:
				fmt.Fprintf(os.Stderr, "consumer error: %v\n", e)

			default:
				fmt.Printf("ignored event: %v\n", e)
			}
		}
	}
}
