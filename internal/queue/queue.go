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

	con := make(chan struct{}, 10) //10 concurrent workers

	sub, err := n.conn.Subscribe("Payroll-Queue", func(m *nats.Msg) {
		con <- struct{}{} //Uses a free worker
		go func() {
			defer func() { <-con }() //Releases a worker
			handleMessage(m.Data)
		}()
	})

	//Alternative method would be to run the worker in a go routine.

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
	n.Close()
	return nil
}
