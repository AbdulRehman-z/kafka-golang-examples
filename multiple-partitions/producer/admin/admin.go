package main

import (
	"context"
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	// Create a new Kafka admin client
	admin, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092", // Kafka broker address
		"client.id":         "admin",          // Client ID for the admin client
	})
	if err != nil {
		fmt.Println(err)
		log.Fatal(kafka.NewError(kafka.ErrApplication, "failed to initiate producer", true))
	}

	// Define the topic to be created
	topic := make([]kafka.TopicSpecification, 0)

	// Create the topic using the admin client
	admin.CreateTopics(context.Background(), append(topic, kafka.TopicSpecification{
		Topic: "players", // Name of the topic to be created
	}))

	// Define the partitions to be added to the topic
	partitions := make([]kafka.PartitionsSpecification, 0)

	// Increase the number of partitions for the topic using the admin client
	admin.CreatePartitions(context.Background(), append(partitions, kafka.PartitionsSpecification{
		Topic:      "players", // Name of the topic
		IncreaseTo: 3,         // Number of partitions to increase to
	}))

	// Keep the program running indefinitely
	select {}
}
