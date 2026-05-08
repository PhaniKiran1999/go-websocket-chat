package repository

import (
	"context"
)

type ConnectionRegistry interface {
	Register(ctx context.Context, clientName, serverID string) error
	UnRegister(ctx context.Context, clientName string) error
	HeartBeat(ctx context.Context, serverID string) error
	GetServerByClient(ctx context.Context, clientName string) (string, error)
	GetValueByKey(ctx context.Context, key string) (string, error)
	GetAllKeys(ctx context.Context) (map[string]string, error)
}
