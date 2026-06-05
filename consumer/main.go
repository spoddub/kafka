package main

import (
	"fmt"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {
	if len(os.Args) < 3 {
		fmt.Println("usage: go run main.go <bootstrap-server> <topic>")
		fmt.Println("example: go run main.go localhost:9094 orders")
		os.Exit(1)
	}

	bootstrapServers := os.Args[1]
	topic := os.Args[2]

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
		"group.id":          "myGroup",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		panic(err)
	}
	defer c.Close()

	err = c.SubscribeTopics([]string{topic}, nil)
	if err != nil {
		panic(err)
	}

	for {
		msg, err := c.ReadMessage(time.Second)

		if err == nil {
			fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
			continue
		}

		kafkaErr, ok := err.(kafka.Error)
		if ok && kafkaErr.IsTimeout() {
			continue
		}

		fmt.Printf("Consumer error: %v\n", err)
	}
}
