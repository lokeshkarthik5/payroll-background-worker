package queue

import (
	"context"

	amq "github.com/rabbitmq/amqp091-go"
)

type Message struct {
	body     []byte
	delivery uint64
}

type Rabbitmq struct {
	conn    *amq.Connection
	channel *amq.Channel
	queue   string
}

type Config struct {
	URL        string
	Exchange   string
	Queue      string
	DLQ        string
	RoutingKey string
	Prefetch   int
}

type Queue interface {
	Enqueue(ctx context.Context, body []byte) error
	Dequeue(ctx context.Context) (Message, error)
	Ack(ctx context.Context, msg Message) error
	Nack(ctx context.Context, msg Message, requeue bool) error
	Close() error
}

func New(cfg Config) (*Rabbitmq, error) {

	conn, err := amq.Dial(cfg.URL)
	if err != nil {
		return nil, err
	}
	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, err
	}
	return &Rabbitmq{
		conn:    conn,
		channel: ch,
		queue:   cfg.Queue,
	}, nil
}
