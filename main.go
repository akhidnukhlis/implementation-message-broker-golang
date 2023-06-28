package main

import (
	"fmt"
	"implementation-message-broker-golang/config"
	"implementation-message-broker-golang/usecase"
	"log"
)

func main() {
	var (
		topic = "test-topic"
		//message = "test-message"
		groupID = "my-consumer-group"
	)

	config, err := config.ReadConfigFromFile()
	if err != nil {
		log.Fatalf("Failed to read config: %s", err)
	}

	// Buat instance untuk publish service
	//publishService := usecase.NewPublishService(config)
	//result, err := publishService.Publish(topic, message)
	//if err != nil {
	//	log.Fatalf("Failed to publish message: %s", err)
	//}
	//fmt.Println(result)

	subscriberService := usecase.NewSubscriberService(config)
	results, err := subscriberService.Subscriber(topic, groupID)
	if err != nil {
		log.Fatalf("Failed subscribe message: %s", err)
	}
	fmt.Println(results)

	<-make(chan struct{})
}
