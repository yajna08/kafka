package main

import (
	// "fmt"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
)

const (
	brokerAddress = "localhost:9092"
	sourceTopic   = "comments"
	dlqTopic      = "comments-dlq"
	groupID       = "dlq-consumer-group"
)

func la() {
	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true

	// Set up the consumer group
	consumer, err := cluster.NewConsumer([]string{brokerAddress}, groupID, []string{sourceTopic}, config)
	if err != nil {
		log.Fatalf("Failed to create consumer group: %v", err)
	}
	defer consumer.Close()
	fmt.Println("Consumer started ")
	// Set up the DLQ producer
	producer, err := sarama.NewSyncProducer([]string{brokerAddress}, nil)
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	fmt.Println("Producer started ")
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	// Start consuming messages from the source topic
	i := 0
	go func() {
		for {
			fmt.Println("Inside select", i)
			


			select {
				

			case msg, ok := <-consumer.Messages():
				fmt.Println("The message is",msg)
				fmt.Println("Inside case", i)
				if ok {
					// Check if the message has been in the queue for more than 5 minutes
					if time.Since(msg.Timestamp) > 5*time.Minute {
						// Produce the message to the DLQ topic
						_, _, err = producer.SendMessage(&sarama.ProducerMessage{
							Topic: dlqTopic,
							Value: sarama.ByteEncoder(msg.Value),
						})
						if err != nil {
							log.Printf("Failed to produce message to DLQ: %v", err)
						} else {
							log.Println("Message sent to DLQ:", string(msg.Value))
						}
					} else {
						log.Println("Processing message:", string(msg.Value))
						// Process the message
						// ...
					}
					// Mark the message as processed
					consumer.MarkOffset(msg, "")
				}
			case err := <-consumer.Errors():
				log.Printf("Consumer error: %v", err)
			case ntf := <-consumer.Notifications():
				log.Printf("Rebalanced: %+v", ntf)
			case <-signals:
				return
			}
			i++
		}
	}()

	// Wait for an interrupt signal to gracefully stop the consumer
	<-signals
	log.Println("Stopping consumer...")
}