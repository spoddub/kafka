package main

import (
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/jsonschema"
)

type Product struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

func main() {
	srClient, err := schemaregistry.NewClient(
		schemaregistry.NewConfig("http://localhost:8081"),
	)
	if err != nil {
		log.Fatalf("failed to create schema registry client: %v", err)
	}

	serializer, err := jsonschema.NewSerializer(
		srClient,
		serde.ValueSerde,
		jsonschema.NewSerializerConfig(),
	)
	if err != nil {
		log.Fatalf("failed to create JSON schema serializer: %v", err)
	}

	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "127.0.0.1:9094",
	})
	if err != nil {
		log.Fatalf("failed to create Kafka producer: %v", err)
	}
	defer producer.Close()

	topic := "products-schema"

	product := Product{
		ID:   30,
		Name: "Product",
	}

	payload, err := serializer.Serialize(topic, &product)
	if err != nil {
		log.Fatalf("failed to serialize payload: %v", err)
	}

	deliveryChan := make(chan kafka.Event)
	defer close(deliveryChan)

	err = producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Key:   []byte("product-key"),
		Value: payload,
	}, deliveryChan)
	if err != nil {
		log.Fatalf("failed to produce message: %v", err)
	}

	event := <-deliveryChan
	message := event.(*kafka.Message)

	if message.TopicPartition.Error != nil {
		log.Fatalf("delivery failed: %v", message.TopicPartition.Error)
	}

	fmt.Printf("Message delivered to %v\n", message.TopicPartition)
}
