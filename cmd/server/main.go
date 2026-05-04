package main

import (
	"log"
	"net/http"

	"github.com/PhaniKiran1999/go-ws-server-app/internal/config"
	"github.com/PhaniKiran1999/go-ws-server-app/internal/messaging"
	"github.com/PhaniKiran1999/go-ws-server-app/internal/repository"
	"github.com/PhaniKiran1999/go-ws-server-app/internal/websocket"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

func main() {

	cfg := config.Load()

	redisClient := redis.NewClient(&redis.Options{
		Addr: cfg.RedisAddr,
		DB:   cfg.RedisDB,
	})

	registry := repository.NewRedisRegistry(redisClient)
	pubsub := messaging.NewRedisPubSub(redisClient)
	hub := websocket.NewHub()

	serverID := uuid.NewString()
	server := websocket.NewServer(serverID, hub, registry, pubsub)

	http.HandleFunc("/ws", server.HandleWebSocket)
	http.HandleFunc("/connectedClients", server.GetConnectedClients)
	http.HandleFunc("/sendMessageTo", server.SendMessageTo)

	log.Println("server listening on", cfg.HTTPAddr)
	log.Fatal(http.ListenAndServe(cfg.HTTPAddr, nil))
}
