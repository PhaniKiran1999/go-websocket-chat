package websocket

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/PhaniKiran1999/go-ws-server-app/internal/messaging"
	"github.com/PhaniKiran1999/go-ws-server-app/internal/repository"
	"github.com/PhaniKiran1999/go-ws-server-app/internal/utils"
	"github.com/google/uuid"
	gorilla "github.com/gorilla/websocket"
)

type Server struct {
	ctx      context.Context
	ID       string
	hub      *Hub
	upgrader gorilla.Upgrader
	registry repository.ConnectionRegistry
	pubsub   messaging.PubSub
}

func NewServer(serverID string, hub *Hub, registry repository.ConnectionRegistry, pubsub messaging.PubSub) *Server {
	ctx := context.Background()
	server := &Server{
		ctx: ctx,
		ID:  serverID,
		hub: hub,
		upgrader: gorilla.Upgrader{
			CheckOrigin: func(r *http.Request) bool {
				return true // Restrict origins before using this in production.
			},
		},
		registry: registry,
		pubsub:   pubsub,
	}

	go server.startHeartbeat()

	msgCh, err := pubsub.Subscribe(ctx, utils.ConstructRedisChannelKey(serverID))
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		for message := range msgCh {
			payload, err := json.Marshal(message)
			if err != nil {
				log.Printf("unable to marshal message: %v", err)
				continue
			}
			err = hub.sendTo(message.RecipientName, payload)
			if err != nil {
				log.Printf("unable to forward message: %v", err)
			}
		}
	}()

	return server
}

func (s *Server) startHeartbeat() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		if err := s.registry.HeartBeat(s.ctx, s.ID); err != nil {
			log.Printf("heartbeat failed: %v", err)
		}

		select {
		case <-s.ctx.Done():
			log.Printf("stopping heartbeat: context cancelled")
			return
		case <-ticker.C:
			log.Printf("refreshed server[%s] on redis", s.ID)
			continue
		}
	}
}

func (s *Server) HandleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("upgrade error: %v", err)
		return
	}

	client := &Client{
		ID:   uuid.New().String(),
		Name: r.Header.Get("client_name"),
		Conn: conn,
		Send: make(chan []byte, 16),
	}

	s.hub.Add(client)
	if err := s.registry.Register(s.ctx, client.Name, s.ID); err != nil {
		log.Fatalf("unable to register client:%v", err)
	}

	log.Printf("[+] connected: %s (total: %d)", client.ID, s.hub.Count())

	if err := conn.WriteMessage(gorilla.TextMessage, []byte("your-id: "+client.ID)); err != nil {
		log.Printf("unable to send client id to %s: %v", client.ID, err)
		s.hub.Remove(client)
		return
	}

	go client.writePump()

	defer func() {
		s.hub.Remove(client)
		s.registry.UnRegister(s.ctx, client.Name)
		_ = conn.Close()
		log.Printf("[-] disconnected: %s (total: %d)", client.ID, s.hub.Count())
	}()

	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			log.Printf("read error: %v", err)
			return
		}

		log.Printf("Message From [%s]: %s", client.ID, data)

		var message messaging.Message
		if err := json.Unmarshal(data, &message); err != nil {
			log.Printf("unable to parse message: %v", err)
			continue
		}

		//retrive serverID
		recipientServerID, err := s.registry.GetServerByClient(s.ctx, message.RecipientName)
		if err != nil {
			log.Printf("unable to find recipientServerID: %v", err)
			continue
		}

		// check if that server is alive
		_, err = s.registry.GetValueByKey(s.ctx, recipientServerID)
		if err != nil {
			log.Printf("recipient server instance is offline: %v", err)
			continue
		}

		if err := s.pubsub.Publish(s.ctx, utils.ConstructRedisChannelKey(recipientServerID), message); err != nil {
			log.Printf("unable to forward message: %v", err)
			continue
		}
	}
}

func (s *Server) GetConnectedClients(w http.ResponseWriter, r *http.Request) {
	// clientIDs := s.connectedClientIDs()
	connectionRegistryMap, err := s.registry.GetAllKeys(s.ctx)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	var names = make([]string, 0, len(connectionRegistryMap))
	for key, _ := range connectionRegistryMap {
		names = append(names, key)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(names); err != nil {
		log.Printf("unable to encode connected clients: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}
}

func (s *Server) SendMessageTo(w http.ResponseWriter, r *http.Request) {
	data, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	var message messaging.Message
	if err := json.Unmarshal(data, &message); err != nil {
		log.Printf("unable to parse message: %v", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	//retrive serverID
	recipientServerID, err := s.registry.GetServerByClient(s.ctx, message.RecipientName)
	if err != nil {
		log.Printf("unable to find recipientServerID: %v", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// check if that server is alive
	_, err = s.registry.GetValueByKey(s.ctx, recipientServerID)
	if err != nil {
		log.Printf("recipient server instance is offline: %v", err)
		w.WriteHeader(http.StatusBadGateway) // not sure what error code to return
		w.Write([]byte(err.Error()))
		return
	}

	if err := s.pubsub.Publish(s.ctx, utils.ConstructRedisChannelKey(recipientServerID), message); err != nil {
		log.Printf("unable to forward message: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}
	w.WriteHeader(http.StatusAccepted)
}
