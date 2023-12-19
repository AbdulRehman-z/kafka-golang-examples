package main

import (
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  "localhost:9092",
		"group.id":           "myGroup", // Consumer group ID explains which consumer group this consumer belongs to
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": false, // Disable auto commit so that we can commit offsets manually after processing messages
		// "fetch.min.bytes":    100,
		// "fetch.max.wait.ms":  3000,
		// "client.id":          "foo",
	})
	if err != nil {
		log.Fatal(kafka.NewError(kafka.ErrApplication, "failed to initiate consumer", true))
	}

	// Subscribe to the "users" topic for message consumption
	consumer.Subscribe("players", nil)

	// Continuously consume messages from the subscribed topic
	for {
		// Read a message from the "users" topic. The -1 value
		// tells the consumer to block indefinitely until a
		// message is received or an error is encountered
		msg, err := consumer.ReadMessage(-1)
		if err == nil {
			// Process the received message
			log.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))

			// Commit the offset of the message that has been processed
			// NOTE: This is a blocking call and will block until an
			// error is encountered or the offset is committed
			_, err := consumer.CommitMessage(msg)
			if err != nil {
				log.Println(kafka.NewError(kafka.ErrApplication, "failed to commit message", true))
			}
		} else {
			// Handle any errors encountered while consuming messages
			log.Println(kafka.NewError(kafka.ErrApplication, "failed to read message", true))
		}
	}
}
