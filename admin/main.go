package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

const topicWaitingTime = 60 * time.Second

func main() {
	if len(os.Args) != 5 {
		log.Fatalf(
			"usage: %s <bootstrap-servers> <topic> <partition-count> <replication-factor>\n",
			os.Args[0],
		)
	}

	bootstrapServers := os.Args[1]
	topic := os.Args[2]

	numPartitions, err := strconv.Atoi(os.Args[3])
	if err != nil {
		log.Fatalf("invalid partition count %q: %v\n", os.Args[3], err)
	}

	replicationFactor, err := strconv.Atoi(os.Args[4])
	if err != nil {
		log.Fatalf("invalid replication factor %q: %v\n", os.Args[4], err)
	}

	adminClient, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
	})
	if err != nil {
		log.Fatalf("failed to create admin client: %v\n", err)
	}
	defer adminClient.Close()

	results, err := adminClient.CreateTopics(
		context.Background(),
		[]kafka.TopicSpecification{
			{
				Topic:             topic,
				NumPartitions:     numPartitions,
				ReplicationFactor: replicationFactor,
			},
		},
		kafka.SetAdminOperationTimeout(topicWaitingTime),
	)
	if err != nil {
		log.Fatalf("failed to create topic: %v\n", err)
	}

	for _, result := range results {
		if result.Error.Code() != kafka.ErrNoError {
			fmt.Printf("topic %q was not created: %v\n", result.Topic, result.Error)
			continue
		}

		fmt.Printf("topic %q created successfully\n", result.Topic)
	}
}
