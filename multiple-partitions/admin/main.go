package main

import (
	"context"
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	admin, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"client.id":         "admin",
	})
	if err != nil {
		fmt.Println(err)
		log.Fatal(kafka.NewError(kafka.ErrApplication, "failed to initiate producer", true))
	}

	topic := make([]kafka.TopicSpecification, 0)

	admin.CreateTopics(context.Background(), append(topic, kafka.TopicSpecification{
		Topic: "players",
	}))

	partitions := make([]kafka.PartitionsSpecification, 0)

	admin.CreatePartitions(context.Background(), append(partitions, kafka.PartitionsSpecification{
		Topic:      "players",
		IncreaseTo: 3,
	}))

	select {}

}
