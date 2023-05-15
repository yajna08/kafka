package main

import (
	"fmt"
	"log"
	"time"

	"github.com/Shopify/sarama"
)


const (
	brokerAddress = "localhost:9092"
	sourceTopic   = "comments"
	dlqTopic      = "comments-dlq"
)


func setupClient() (sarama.Client, error) {
	config := sarama.NewConfig()

	// Add any necessary configuration options, such as authentication, TLS, etc.

	// Create the Kafka client
	client, err := sarama.NewClient([]string{brokerAddress}, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka client: %v", err)
	}

	return client, nil
}

func createDLQ() error {
	// Set up the Kafka client
	client, err := setupClient()
	if err != nil {
		return err
	}
	defer client.Close()

	// Create a consumer for the source topic
	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		return fmt.Errorf("failed to create consumer: %v", err)
	}
	defer consumer.Close()

	// Create a producer for the DLQ topic
	producer, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		return fmt.Errorf("failed to create producer: %v", err)
	}
	defer producer.Close()

	// Set up the topic partitions to consume from
	partitions, err := consumer.Partitions(sourceTopic)
	if err != nil {
		return fmt.Errorf("failed to retrieve partitions: %v", err)
	}

	// Consume messages from the source topic and produce to the DLQ topic
	for _, partition := range partitions {
		// Create a consumer for the partition
		partitionConsumer, err := consumer.ConsumePartition(sourceTopic, partition, sarama.OffsetOldest)
		if err != nil {
			log.Printf("Failed to consume partition %d: %v", partition, err)
			continue
		}
		defer partitionConsumer.Close()

		// Consume and produce messages
		for msg := range partitionConsumer.Messages() {
				// Check if the message has been in the queue for more than 5 minutes
				if time.Since(msg.Timestamp) > 5*time.Minute {
					// Produce the message to the DLQ topic
					_, _, err = producer.SendMessage(&sarama.ProducerMessage{
						Topic: dlqTopic,
						Value: sarama.ByteEncoder(msg.Value),
					})
					if err != nil {
						log.Printf("Failed to produce message to DLQ: %v", err)
						continue
					}

					// Mark the original message as processed
					partitionConsumer.MarkOffset(msg, "")
				} else {
					// Process the message
					// ...
				}
			}
		}
	

	// fmt.Println("Processed", msgCount, "messages")
	return nil
}

