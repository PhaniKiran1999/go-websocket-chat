package websocket

import (
	"fmt"
	"sync"
)

type Hub struct {
	mu      sync.RWMutex
	clients map[string]*Client
}

func NewHub() *Hub {
	return &Hub{
		clients: make(map[string]*Client),
	}
}

func (h *Hub) Add(client *Client) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.clients[client.Name] = client
}

func (h *Hub) Remove(client *Client) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.clients, client.Name)
	close(client.Send)
}

func (h *Hub) Names() []string {
	h.mu.RLock()
	defer h.mu.RUnlock()

	names := make([]string, 0, len(h.clients))
	for _, client := range h.clients {
		names = append(names, client.Name)
	}
	return names
}

func (h *Hub) Count() int {
	h.mu.RLock()
	defer h.mu.RUnlock()

	return len(h.clients)
}

func (h *Hub) sendTo(recipientName string, message []byte) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	client, ok := h.clients[recipientName]
	if !ok {
		return fmt.Errorf("client with name %q does not exist", recipientName)
	}

	select {
	case client.Send <- message:
		return nil
	default:
		// The use of select and default follows "Non-Blocking Send implementation" prevents system from slowing if the client is slow in responding
		// channel is full. so, we drop the message to keep the system fast.
		return fmt.Errorf("client with name %q is not ready to receive messages", recipientName)
	}
}
