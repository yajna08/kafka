package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"
	// producer "kafka/producer"
	"github.com/Shopify/sarama"
)

func main() {

	topic := "comments"
	worker, err := connectConsumer([]string{"localhost:9092"})
	if err != nil {
		panic(err)
	}

	// Calling ConsumePartition. It will open one connection per broker
	// and share it for all partitions that live on it.
	consumer, err := worker.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		panic(err)
	}
	fmt.Println("Consumer started ")
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	// Count how many message processed
	msgCount := 0

	// Get signal for finish
	doneCh := make(chan struct{})
	go func() {
		for {
			select {
			case err := <-consumer.Errors():
				fmt.Println(err)
			case msg := <-consumer.Messages():

				msgCount++
				currentTimeInSeconds := time.Now().Day()*86400 + time.Now().Hour()*3600 + time.Now().Minute()*60 + time.Now().Second()
				messageTimeInSeconds := msg.Timestamp.Day()*86400 + msg.Timestamp.Hour()*3600 + msg.Timestamp.Minute()*60 + msg.Timestamp.Second()

				fmt.Println(messageTimeInSeconds)
				fmt.Println(currentTimeInSeconds)
				if currentTimeInSeconds-messageTimeInSeconds > 8000 {
					fmt.Println("This message is to be pushed to dead letter queue", string(msg.Value))
					
				}

				deltaTime, time := timeFormat(currentTimeInSeconds - messageTimeInSeconds)
				fmt.Println(deltaTime)
				fmt.Println("The minutes difference is ", time.Minute())

			case <-sigchan:

				fmt.Println("Interrupt is detected")
				doneCh <- struct{}{}

			}
		}
	}()

	<-doneCh
	fmt.Println("Processed", msgCount, "messages")

	if err := worker.Close(); err != nil {
		panic(err)
	}
	// dlq.createDLQ()
}

func connectConsumer(brokersUrl []string) (sarama.Consumer, error) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	// Create new consumer
	conn, err := sarama.NewConsumer(brokersUrl, config)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

func difference(time1Str, time2Str string) (string, error) {
	fmt.Println("The time1 is ", time1Str)
	time1, err := time.Parse("2006-01-02 15:04:05.000 +0530 IST", time1Str)
	if err != nil {
		return "", fmt.Errorf("error parsing time1: %v", err)
	}

	fmt.Println("The time is ", time1)
	time2, err := time.Parse("2006-01-02 15:04:05.000 +0530 IST", time2Str)
	if err != nil {
		return "", fmt.Errorf("error parsing time2: %v", err)
	}

	diff := time2.Sub(time1)
	fmt.Println("The difference is", diff)
	return string(diff), nil
}

func timeFormat(second int) (string, time.Time) {
	// add day also
	day := second / 86400
	hour := (second - day*86400) / 3600
	minute := (second - hour*3600 - day*86400) / 60
	second = second - hour*3600 - minute*60 - day*86400
	return fmt.Sprintf("%d:%d:%d:%d", day, hour, minute, second), time.Date(0, 0, day, hour, minute, second, 0, time.UTC)
	// return time.Date(0, 0, day, hour, minute, second, 0, time.UTC)
}

// 52-19 = 33
