package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	// Create a new Kafka producer
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":  "localhost:9092", // Kafka broker address
		"client.id":          "fooBar",         // Client ID for the producer
		"acks":               "all",            // Wait for all in-sync replicas to ack the message
		"enable.idempotence": true,             // Enable idempotence to avoid duplicate messages
	})
	if err != nil {
		log.Fatal(kafka.NewError(kafka.ErrProducerFenced, "failed to initiate producer", true))
	}

	// Channel to receive delivery reports for produced messages
	deliveryChan := make(chan kafka.Event, 1000)

	// Kafka topic to produce messages to
	topic := "players"

	// Goroutine to handle delivery reports for produced messages
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

	// Generate 1000 random player names
	players := []string{"ali", "reza", "mohammad", "hassan", "hossein", "sajad", "mehdi", "ahmad", "mohsen", "morteza"}
	for i := 0; i < 1000; i++ {
		players = append(players, players[i%10])
	}

	// Produce 1000 messages to the Kafka topic
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		go produce(producer, players, topic, deliveryChan, int32(i), &wg)
		wg.Add(1)
	}
	wg.Wait()

	// Wait for all messages in the delivery channel to be delivered
	i := producer.Flush(1000 * 1000)
	if i == 0 {
		fmt.Printf("All messages successfully delivered\n")
		close(deliveryChan)
	} else {
		fmt.Printf("%d messages were not delivered\n", i)
	}
}

func produce(producer *kafka.Producer, players []string, topic string, deliveryChan chan kafka.Event, partitionNum int32, wg *sync.WaitGroup) {
	for i := 0; i < 1000; i++ {
		time.Sleep(1 * time.Second)

		// Select a player name
		player := players[i]
		msg := &kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &topic,
				Partition: partitionNum,
			},
			Value:  []byte(player),
			Opaque: "hello",
		}
		// Produce the message and handle any errors
		err := producer.Produce(msg, deliveryChan)
		if err != nil {
			log.Fatal(kafka.NewError(kafka.ErrProducerFenced, "failed to produce events", true))
		}
	}
	wg.Done()
}
