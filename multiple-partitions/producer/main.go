package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {

	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":  "localhost:9092",
		"client.id":          "fooBar",
		"acks":               "all", // Wait for all in-sync replicas to ack the message
		"enable.idempotence": true,  // Enable idempotence, meaning that the producer will not send duplicate messages in case of retries
	})
	if err != nil {
		log.Fatal(kafka.NewError(kafka.ErrProducerFenced, "failed to initiate producer", true))
	}

	// Channel to receive delivery reports for produced messages
	deliveryChan := make(chan kafka.Event, 1000)

	// Kafka topic to produce messages to
	topic := "players"

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

	// generate 1000 random players names
	players := []string{"ali", "reza", "mohammad", "hassan", "hossein", "sajad", "mehdi", "ahmad", "mohsen", "morteza"}
	for i := 0; i < 1000; i++ {
		players = append(players, players[i%10])
	}

	// Produce 1000 messages to the Kafka topic
	wg := sync.WaitGroup{}

	for i := 0; i < 3; i++ {
		fmt.Print(i)
		go produce(producer, players, topic, deliveryChan, int32(i), &wg)
		wg.Add(1)
	}

	wg.Wait()

	// // Wait for all messages in the delivery channel to be delivered
	i := producer.Flush(1000 * 1000)
	if i == 0 {
		fmt.Printf("All messages successfully delivered\n")
		close(deliveryChan)

	} else {
		fmt.Printf("%d messages were not delivered\n", i)
	}

	// select {}
}

func produce(producer *kafka.Producer, players []string, topic string, deliveryChan chan kafka.Event, partitionNum int32, wg *sync.WaitGroup) {
	// wg.Wait()
	for i := 0; i < 1000; i++ {
		time.Sleep(1 * time.Second)

		// select a player name
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

// func producer3(producer *kafka.Producer, players []string, topic string, deliveryChan chan kafka.Event, wg *sync.WaitGroup) {
// 	// wg.Wait()
// 	for i := 0; i < 1000; i++ {
// 		time.Sleep(1 * time.Second)

// 		// select a player name
// 		player := players[i]
// 		msg := &kafka.Message{
// 			TopicPartition: kafka.TopicPartition{
// 				Topic:     &topic,
// 				Partition: 2,
// 			},
// 			Value:  []byte(player),
// 			Opaque: "hello",
// 		}
// 		// Produce the message and handle any errors
// 		err := producer.Produce(msg, deliveryChan)
// 		if err != nil {
// 			log.Fatal(kafka.NewError(kafka.ErrProducerFenced, "failed to produce events", true))
// 		}
// 	}
// 	wg.Done()
// }
