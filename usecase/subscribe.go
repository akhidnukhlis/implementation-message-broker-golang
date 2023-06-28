package usecase

import (
	"errors"
	"fmt"
	"implementation-message-broker-golang/config"
	"implementation-message-broker-golang/entity"
	"implementation-message-broker-golang/provider"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
)

type SubscriberService struct {
	config *config.Config
}

func NewSubscriberService(config *config.Config) *SubscriberService {
	return &SubscriberService{
		config: config,
	}
}

func (g *SubscriberService) Subscriber(topic, groupID string) (*entity.Response, error) {
	var result entity.Response

	switch g.config.Env {
	case "dev":
		// Inisialisasi Apache Kafka Broker
		consumer, err := provider.NewKafkaConsumer(g.config.KafkaEndpoint, groupID, topic)
		if err != nil {
			log.Fatal("Failed to create Kafka message consumer:", err)
		}
		defer func() {
			if err := consumer.Close(); err != nil {
				log.Println("Failed to close Kafka consumer:", err)
			}
		}()

		messageChan, errorChan := consumer.ConsumehMessage()

		stopChan := make(chan os.Signal, 1)
		signal.Notify(stopChan, os.Interrupt)

		var wg sync.WaitGroup
		wg.Add(1)

		go func() {
			defer wg.Done()

			for {
				select {
				case message := <-messageChan:
					result.Code = http.StatusOK
					result.Message = "Success"
					result.Data = fmt.Sprintf("Received message: key=%s, value=%s\n", string(message.Key), string(message.Value))
				case err := <-errorChan:
					log.Println("Error while consuming message:", err)
				case <-stopChan:
					return
				}
			}
		}()

		wg.Wait()

		return &result, nil
	case "prod":
		pubsubBroker, err := provider.NewPubSubBroker(g.config.GoogleApplicationCredentials)
		if err != nil {
			log.Fatalf("Failed to create Pub/Sub broker: %v", err)
		}

		result, err := pubsubBroker.Subscribe(topic)
		if err != nil {
			log.Fatalf("Failed to subscribe to Pub/Sub topic: %v", err)
		}

		return result, nil
	default:
		return nil, errors.New("gagal tidak ada layanan aktif yang ditentukan")
	}

	return nil, nil
}
