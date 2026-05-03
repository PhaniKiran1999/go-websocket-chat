package main

import (
	"log"
	"net/http"

	"github.com/PhaniKiran1999/go-ws-server-app/internal/websocket"
)

func main() {
	server := websocket.NewServer()

	http.HandleFunc("/ws", server.HandleWebSocket)
	http.HandleFunc("/connectedClients", server.GetConnectedClients)
	http.HandleFunc("/sendMessageTo", server.SendMessageTo)

	log.Println("server listening on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
