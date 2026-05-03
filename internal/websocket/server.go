package websocket

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"

	"github.com/google/uuid"
	gorilla "github.com/gorilla/websocket"
)

type Server struct {
	clients  map[string]*Client
	mu       sync.RWMutex
	upgrader gorilla.Upgrader
}

func NewServer() *Server {
	return &Server{
		clients: make(map[string]*Client),
		upgrader: gorilla.Upgrader{
			CheckOrigin: func(r *http.Request) bool {
				return true // Restrict origins before using this in production.
			},
		},
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

	s.addClient(client)
	log.Printf("[+] connected: %s (total: %d)", client.ID, s.clientCount())

	if err := conn.WriteMessage(gorilla.TextMessage, []byte("your-id: "+client.ID)); err != nil {
		log.Printf("unable to send client id to %s: %v", client.ID, err)
		s.removeClient(client)
		return
	}

	go client.writePump()

	defer func() {
		s.removeClient(client)
		_ = conn.Close()
		log.Printf("[-] disconnected: %s (total: %d)", client.ID, s.clientCount())
	}()

	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			log.Printf("read error: %v", err)
			return
		}

		log.Printf("Message From [%s]: %s", client.ID, msg)

		var message Message
		if err := json.Unmarshal(msg, &message); err != nil {
			log.Printf("unable to parse message: %v", err)
			continue
		}

		if err := s.sendTo(message.ID, []byte(message.Msg)); err != nil {
			log.Printf("unable to forward message: %v", err)
		}
	}
}

func (s *Server) GetConnectedClients(w http.ResponseWriter, r *http.Request) {
	clientIDs := s.connectedClientIDs()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	if err := json.NewEncoder(w).Encode(clientIDs); err != nil {
		log.Printf("unable to encode connected clients: %v", err)
	}
}

func (s *Server) SendMessageTo(w http.ResponseWriter, r *http.Request) {
	clientID := r.Header.Get("client_id")
	if clientID == "" {
		http.Error(w, "client_id header is required", http.StatusBadRequest)
		return
	}

	data, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	if err := s.sendTo(clientID, data); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.WriteHeader(http.StatusAccepted)
}

func (s *Server) addClient(client *Client) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.clients[client.ID] = client
}

func (s *Server) removeClient(client *Client) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.clients[client.ID]; ok {
		delete(s.clients, client.ID)
		close(client.Send)
	}
}

func (s *Server) connectedClientIDs() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	clientIDs := make([]string, 0, len(s.clients))
	for _, client := range s.clients {
		clientIDs = append(clientIDs, client.ID)
	}

	return clientIDs
}

func (s *Server) clientCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return len(s.clients)
}

func (s *Server) sendTo(clientID string, msg []byte) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	client, ok := s.clients[clientID]
	if !ok {
		return fmt.Errorf("client with %s does not exist", clientID)
	}

	select {
	case client.Send <- msg:
		return nil
	default:
		// The use of select and default follows "Non-Blocking Send implementation" prevents system from slowing if the client is slow in responding
		// channel is full. so, we drop the message to keep the system fast.
		return fmt.Errorf("client with %s is not ready to receive messages", clientID)
	}
}
