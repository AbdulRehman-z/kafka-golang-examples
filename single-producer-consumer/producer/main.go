package main

import (
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	// Create a new Kafka producer instance
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":  "localhost:9092",
		"client.id":          "foo",
		"acks":               "all", // Wait for all in-sync replicas to ack the message
		"enable.idempotence": true,  // Enable idempotence, meaning that the producer will not send duplicate messages in case of retries
	})
	if err != nil {
		// Handle any errors during producer creation
		err = kafka.NewError(kafka.ErrProducerFenced, "failed to initiate producer", true)
		log.Fatal(err)
	}

	// Channel to receive delivery reports for produced messages
	deliveryChan := make(chan kafka.Event, 1000)

	// Goroutine to handle delivery reports for produced messages
	// NOTE: We launch this goroutine before the loop below so that
	// we don't miss any delivery reports while the producer is
	// busy producing messages in the loop below if we launch the go
	// routine after the loop below then we will have to wait until
	// the loop below is finished.
	go func() {
		for e := range deliveryChan {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Failed to deliver message: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("Successfully produced record to topic %s partition [%d] @ offset %v\n",
						*ev.TopicPartition.Topic, ev.TopicPartition.Partition, ev.TopicPartition.Offset)
				}
			}
		}
	}()

	// generate 1000 random users names
	users := []string{"ali", "reza", "mohammad", "hassan", "hossein", "sajad", "mehdi", "ahmad", "mohsen", "morteza"}
	for i := 0; i < 1000; i++ {
		users = append(users, users[i%10])
	}

	// Kafka topic to produce messages to
	topic := "users"

	// Produce 1000 messages to the Kafka topic
	for i := 0; i < 1000; i++ {

		// select a user name
		user := users[i]
		msg := &kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &topic,
				Partition: kafka.PartitionAny,
			},
			Value:  []byte(user),
			Opaque: "hello",
		}
		// Produce the message and handle any errors
		err := producer.Produce(msg, deliveryChan)
		if err != nil {
			log.Fatal(kafka.NewError(kafka.ErrProducerFenced, "faield to produce events", true))
		}
	}

	// Block indefinitely to keep the application running
	select {}
}
