# Kafka Go Examples

This repository contains examples of Kafka producers and consumers written in Go. The examples are organized into different scenarios:

- `multiple-consumers-single-producer`: This directory contains an example of a single Kafka producer sending messages to a topic that is consumed by multiple consumers. Each consumer is in its own directory (`consumer_1` and `consumer_2`).

- `multiple-partitions`: This directory contains an example of a Kafka topic with multiple partitions. The `admin` directory contains code for creating the topic and partitions. There are three consumers (`consumers_1`, `consumers_2`, and `consumers_3`), each consuming from a different partition.

- `single-producer-consumer`: This directory contains a simple example of a single Kafka producer and a single Kafka consumer.

Each directory contains a `main.go` file which is the entry point for the producer or consumer. The `go.mod` file in each directory specifies the dependencies for that producer or consumer.

## Getting Started

To run these examples, you will need to have Docker and Go installed on your machine.

1. Start the Kafka broker by running `docker-compose up` from the root directory.

2. Navigate to the directory of the example you want to run (e.g., `cd multiple-consumers-single-producer/producer`).

3. Run the producer or consumer with `go run main.go`.
