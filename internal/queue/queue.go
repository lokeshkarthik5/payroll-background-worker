package queue

import (
	"context"
	"log"

	"github.com/nats-io/nats.go"
)

type Nats struct {
	conn *nats.Conn
}

type Queue interface {
	Enqueue(ctx context.Context, body []byte) error
	Consume(ctx context.Context) error
	Close() error
}

func New(url string) (*Nats, error) {
	nc, err := nats.Connect(url)
	if err != nil {
		return nil, err
	}

	return &Nats{
		conn: nc,
	}, nil
}

func (n *Nats) Enqueue(ctx context.Context, body []byte) error {
	err := n.conn.Publish("Payroll-Queue", body)

	if err != nil {
		log.Println("Cannot add it in queue")
		return err
	}

	return nil
}

func (n *Nats) Consume(ctx context.Context) error {

	con := make(chan struct{}, 10)

	sub, err := n.conn.Subscribe("Payroll-Queue", func(m *nats.Msg) {
		con <- struct{}{}
		go func() {
			defer func() { <-con }()
			handleMessage(m.Data)
		}()
	})

	if err != nil {
		log.Println("Error getting message")
		return err
	}

	go func() {
		<-ctx.Done()
		sub.Unsubscribe()
	}()
	return nil
}

func (n *Nats) Close() error {
	defer n.Close()
	return nil
}
