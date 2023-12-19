package main

import (
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	// Create a new Kafka consumer instance
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          "foo",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		// Handle any errors during consumer creation
		log.Fatal(kafka.NewError(kafka.ErrApplication, "failed to initiate consumer", true))
	}

	// Subscribe to the "users" topic for message consumption
	consumer.Subscribe("users", nil)

	// Continuously consume messages from the subscribed topic
	for {
		// Read a message from the "users" topic. The -1 value
		// tells the consumer to block indefinitely until a
		// message is received or an error is encountered
		msg, err := consumer.ReadMessage(-1)
		if err == nil {
			// Process the received message
			log.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
		} else {
			// Handle any errors encountered while consuming messages
			log.Println(kafka.NewError(kafka.ErrApplication, "failed to read message", true))
		}
	}
}
