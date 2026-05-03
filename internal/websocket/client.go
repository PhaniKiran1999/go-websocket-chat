package websocket

import (
	"log"

	gorilla "github.com/gorilla/websocket"
)

type Client struct {
	ID   string
	Name string
	Conn *gorilla.Conn
	Send chan []byte
}

func (c *Client) writePump() {
	for msg := range c.Send {
		if err := c.Conn.WriteMessage(gorilla.TextMessage, msg); err != nil {
			log.Printf("unable to send message to client[%s]: %v", c.ID, err)
			return
		}
	}
}
