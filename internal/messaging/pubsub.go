package messaging

import "context"

type PubSub interface {
	Publish(ctx context.Context, channelName string, message Message) error
	Subscribe(ctx context.Context, channelName string) (<-chan Message, error)
	Close() error
}
