package main

import (
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func NewAdminClient() (*kafka.AdminClient, error) {
	admin, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"boootstrap.servers": "localhost:9092",
		"client.id":          "admin",
	})
	if err != nil {
		log.Fatal(kafka.NewError(kafka.ErrApplication, "failed to initiate producer", true))
	}

	return admin, nil
}
