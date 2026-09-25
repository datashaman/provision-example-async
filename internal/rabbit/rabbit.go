package rabbit

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/datashaman/provision-example-async/internal/example"
	"github.com/datashaman/provision-example-async/internal/worker"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Publisher struct {
	connection *amqp.Connection
	channel    *amqp.Channel
	returns    <-chan amqp.Return
}

func OpenPublisher(urlFile string) (*Publisher, error) {
	connection, channel, err := open(urlFile)
	if err != nil {
		return nil, err
	}
	if err := channel.Confirm(false); err != nil {
		channel.Close()
		connection.Close()
		return nil, fmt.Errorf("enable RabbitMQ publisher confirms: %w", err)
	}
	return &Publisher{connection: connection, channel: channel, returns: channel.NotifyReturn(make(chan amqp.Return, 1))}, nil
}

func (p *Publisher) Publish(ctx context.Context, queue string, message example.Message) (bool, error) {
	if _, err := p.channel.QueueDeclarePassive(queue, true, false, false, false, nil); err != nil {
		return false, fmt.Errorf("verify queue %q: %w", queue, err)
	}
	payload, err := message.Marshal()
	if err != nil {
		return false, err
	}
	confirmation, err := p.channel.PublishWithDeferredConfirmWithContext(ctx, "", queue, true, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		MessageId:    message.ID,
		Type:         example.MessageSchemaVersion,
		Body:         payload,
	})
	if err != nil {
		return false, err
	}
	confirmed, err := confirmation.WaitContext(ctx)
	if err != nil || !confirmed {
		return confirmed, err
	}
	select {
	case returned := <-p.returns:
		if returned.MessageId == message.ID {
			return false, fmt.Errorf("broker returned message %s: %s", message.ID, returned.ReplyText)
		}
	default:
	}
	return true, nil
}

func (p *Publisher) Close() error {
	channelErr := p.channel.Close()
	connectionErr := p.connection.Close()
	if channelErr != nil {
		return channelErr
	}
	return connectionErr
}

type Consumer struct {
	connection *amqp.Connection
	channel    *amqp.Channel
	queue      string
}

func OpenConsumer(urlFile, queue string) (*Consumer, error) {
	connection, channel, err := open(urlFile)
	if err != nil {
		return nil, err
	}
	if _, err := channel.QueueDeclarePassive(queue, true, false, false, false, nil); err != nil {
		channel.Close()
		connection.Close()
		return nil, fmt.Errorf("verify queue %q: %w", queue, err)
	}
	if err := channel.Qos(1, 0, false); err != nil {
		channel.Close()
		connection.Close()
		return nil, fmt.Errorf("set RabbitMQ prefetch: %w", err)
	}
	return &Consumer{connection: connection, channel: channel, queue: queue}, nil
}

func (c *Consumer) Start(tag string) (<-chan worker.Delivery, error) {
	deliveries, err := c.channel.Consume(c.queue, tag, false, false, false, false, nil)
	if err != nil {
		return nil, err
	}
	converted := make(chan worker.Delivery, 1)
	go func() {
		defer close(converted)
		for delivery := range deliveries {
			raw := delivery
			converted <- worker.Delivery{
				Body:      raw.Body,
				MessageID: raw.MessageId,
				Ack:       func() error { return raw.Ack(false) },
				Nack:      func(requeue bool) error { return raw.Nack(false, requeue) },
			}
		}
	}()
	return converted, nil
}

func (c *Consumer) Cancel(tag string) error {
	return c.channel.Cancel(tag, false)
}

func (c *Consumer) Close() error {
	channelErr := c.channel.Close()
	connectionErr := c.connection.Close()
	if channelErr != nil {
		return channelErr
	}
	return connectionErr
}

func open(urlFile string) (*amqp.Connection, *amqp.Channel, error) {
	url, err := readURL(urlFile)
	if err != nil {
		return nil, nil, err
	}
	connection, err := amqp.DialConfig(url, amqp.Config{Properties: amqp.Table{"product": "provision-example-async"}})
	if err != nil {
		return nil, nil, fmt.Errorf("connect to RabbitMQ: %w", err)
	}
	channel, err := connection.Channel()
	if err != nil {
		connection.Close()
		return nil, nil, fmt.Errorf("open RabbitMQ channel: %w", err)
	}
	return connection, channel, nil
}

func readURL(path string) (string, error) {
	if path == "" {
		return "", fmt.Errorf("broker URL credential file is required")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read broker URL credential file: %w", err)
	}
	url := strings.TrimSpace(string(data))
	if url == "" {
		return "", fmt.Errorf("broker URL credential file is empty")
	}
	return url, nil
}
