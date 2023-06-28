package provider

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/google/uuid"
	"implementation-message-broker-golang/entity"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"time"
)

const ()

type MessagePublisher interface {
	Publish(topic string, message string) error
	Close() error
}

type KafkaMessagePublisher struct {
	producer sarama.SyncProducer
}

func NewKafkaPubliser(broker string) (*KafkaMessagePublisher, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true

	producer, err := sarama.NewSyncProducer([]string{broker}, config)
	if err != nil {
		return nil, err
	}

	return &KafkaMessagePublisher{
		producer: producer,
	}, nil
}

func (kmp *KafkaMessagePublisher) PublishMessage(topic string, message string) (*entity.Response, error) {
	var result *entity.Response

	// Generate a new UUID
	uuid := uuid.New()

	// Create a new Sarama string encoder with the UUID string
	key := sarama.StringEncoder(uuid.String())

	msg := &sarama.ProducerMessage{
		Key:   key,
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}

	var wg sync.WaitGroup

	messages := make(chan string)

	ctx, cancel := context.WithCancel(context.Background())

	wg.Add(1)
	go func(ctx context.Context) {
		defer wg.Done()

		partition, offset, err := kmp.producer.SendMessage(msg)
		if err != nil {
			log.Fatalf("Failed to producer: %v", err)
			return
		}

		messages <- fmt.Sprintf("Message sent successfully. Key=%s, Partition=%d, Offset=%d", key, partition, offset)
	}(ctx)

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt)
	go func() {
		<-signalCh
		cancel() // Cancel the context to signal the goroutine to stop
	}()

	time.Sleep(8 * time.Millisecond)

	wg.Wait()

	result.Code = http.StatusOK
	result.Message = "Success"
	result.Data = messages

	return result, nil
}

func (kmp *KafkaMessagePublisher) Close() error {
	return kmp.producer.Close()
}

type MessageConsumer interface {
	Consume() (<-chan *sarama.ConsumerMessage, <-chan error)
	Close() error
}

type KafkaMessageConsumer struct {
	consumer sarama.ConsumerGroup
	topic    string
}

func NewKafkaConsumer(broker, groupID, topic string) (*KafkaMessageConsumer, error) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	consumer, err := sarama.NewConsumerGroup([]string{broker}, groupID, config)
	if err != nil {
		return nil, err
	}

	return &KafkaMessageConsumer{
		consumer: consumer,
		topic:    topic,
	}, nil
}

func (kmc *KafkaMessageConsumer) ConsumehMessage() (<-chan *sarama.ConsumerMessage, <-chan error) {
	messageCh := make(chan *sarama.ConsumerMessage)
	errCh := make(chan error)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	consumerGroupHandler := &consumerGroupHandler{
		messageCh: messageCh,
		errCh:     errCh,
	}

	go func() {
		for err := range kmc.consumer.Errors() {
			errCh <- err
		}
	}()

	topics := []string{kmc.topic}

	go func() {
		for {
			err := kmc.consumer.Consume(ctx, topics, consumerGroupHandler)
			if err != nil {
				errCh <- err
			}
		}
	}()

	return messageCh, errCh
}

func (kmc *KafkaMessageConsumer) Close() error {
	return kmc.consumer.Close()
}

type consumerGroupHandler struct {
	messageCh chan<- *sarama.ConsumerMessage
	errCh     chan<- error
}

func (h *consumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h *consumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h *consumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		h.messageCh <- message

		// Mark offset as consumed
		session.MarkMessage(message, "")
	}

	return nil
}
