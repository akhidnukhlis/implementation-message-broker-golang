package provider

import (
	"cloud.google.com/go/pubsub"
	"context"
	"fmt"
	"implementation-message-broker-golang/entity"
	"log"
	"net/http"
	"time"
)

type Subscriber interface {
	Receive(topic string, message string)
}

type PubSubBroker struct {
	client *pubsub.Client
}

func NewPubSubBroker(projectID string) (*PubSubBroker, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("failed to create Pub/Sub client: %v", err)
	}

	return &PubSubBroker{
		client: client,
	}, nil
}

func (pb *PubSubBroker) Subscribe(topic string) (*entity.Response, error) {
	var result entity.Response

	sub := pb.client.Subscription(topic)

	exists, err := sub.Exists(context.Background())
	if err != nil {
		return nil, err
	}

	if !exists {
		_, err := pb.client.CreateSubscription(context.Background(), topic, pubsub.SubscriptionConfig{
			Topic:       pb.client.Topic(topic),
			AckDeadline: 10,
		})
		if err != nil {
			return nil, err
		}
	}

	go func() {
		err = sub.Receive(context.Background(), func(ctx context.Context, msg *pubsub.Message) {
			result.Data = fmt.Sprintf("Received message: Topic=%s, PublishTime=%d, ID=%s, Value=%s\n", topic, msg.PublishTime, string(msg.ID), string(msg.Data))
			msg.Ack()
		})
		if err != nil {
			log.Fatalf("failed to receive message: %v", err)
		}
	}()

	return &entity.Response{
		Code:    http.StatusOK,
		Message: "Success",
	}, nil
}

func (pb *PubSubBroker) Publish(topic string, message string) {
	t := pb.client.Topic(topic)
	t.Publish(context.Background(), &pubsub.Message{
		Data: []byte(message),
	})
}
