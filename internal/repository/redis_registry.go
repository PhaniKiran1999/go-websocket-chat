package repository

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

const connectionRegistryKey = "connection-registry"

type RedisRegistry struct {
	client *redis.Client
}

func NewRedisRegistry(client *redis.Client) *RedisRegistry {
	return &RedisRegistry{
		client: client,
	}
}

func (r *RedisRegistry) Register(ctx context.Context, clientName, serverID string) error {
	return r.client.HSet(ctx, connectionRegistryKey, clientName, serverID).Err()
}

func (r *RedisRegistry) UnRegister(ctx context.Context, clientName string) error {
	return r.client.HDel(ctx, connectionRegistryKey, clientName).Err()
}

func (r *RedisRegistry) HeartBeat(ctx context.Context, serverID string) error {
	return r.client.Set(ctx, serverID, 0, 5*time.Minute).Err()
}

func (r *RedisRegistry) GetServerByClient(ctx context.Context, key string) (string, error) {
	return r.client.HGet(ctx, connectionRegistryKey, key).Result()
}

func (r *RedisRegistry) GetValueByKey(ctx context.Context, key string) (string, error) {
	return r.client.Get(ctx, key).Result()
}
