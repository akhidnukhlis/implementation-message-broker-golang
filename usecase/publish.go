package usecase

import (
	"errors"
	"implementation-message-broker-golang/config"
	"implementation-message-broker-golang/entity"
	"implementation-message-broker-golang/provider"
	"log"
)

type PublishService struct {
	config *config.Config
}

func NewPublishService(config *config.Config) *PublishService {
	return &PublishService{
		config: config,
	}
}

func (g *PublishService) Publish(topic, message string) (*entity.Response, error) {
	var result *entity.Response

	switch g.config.Env {
	case "dev":
		publisher, err := provider.NewKafkaPubliser(g.config.KafkaEndpoint)
		if err != nil {
			log.Fatal("Failed to create Kafka message publisher:", err)
		}

		res, err := publisher.PublishMessage(topic, message)
		if err != nil {
			log.Println("Failed to publish message:", err)
		}

		return res, nil
	case "prod":
		publisher, err := provider.NewPubSubBroker(g.config.GoogleApplicationCredentials)
		if err != nil {
			log.Fatalf("Failed to create Pub/Sub broker: %v", err)
		}

		publisher.Publish(topic, message)
	default:
		return nil, errors.New("gagal tidak ada layanan aktif yang ditentukan")
	}

	return result, nil
}
