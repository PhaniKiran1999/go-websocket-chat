package messaging

import (
	"context"
	"encoding/json"
	"log"

	"github.com/redis/go-redis/v9"
)

type RedisPubSub struct {
	client *redis.Client
}

func NewRedisPubSub(client *redis.Client) *RedisPubSub {
	return &RedisPubSub{
		client: client,
	}
}

func (p *RedisPubSub) Publish(ctx context.Context, channelName string, message Message) error {
	payload, err := json.Marshal(message)
	if err != nil {
		return err
	}

	return p.client.Publish(ctx, channelName, payload).Err()
}

func (p *RedisPubSub) Subscribe(ctx context.Context, channelName string) (<-chan Message, error) {
	sub := p.client.Subscribe(ctx, channelName)
	if _, err := sub.Receive(ctx); err != nil {
		_ = sub.Close()
		return nil, err
	}

	ch := sub.Channel()
	out := make(chan Message)

	go func() {
		defer close(out)
		defer sub.Close()

		log.Printf("subscriber: waiting for messages on %s", channelName)

		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-ch:
				if !ok {
					return
				}

				var message Message
				if err := json.Unmarshal([]byte(msg.Payload), &message); err != nil {
					log.Printf("subscriber: unable to decode message from %s: %v", msg.Channel, err)
					continue
				}

				select {
				case out <- message:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	return out, nil
}

func (p *RedisPubSub) Close() error {
	return p.client.Close()
}
