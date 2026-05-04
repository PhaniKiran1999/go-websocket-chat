package messaging

type Message struct {
	RecipientName string `json:"recipient_name"`
	Payload       string `json:"body"`
}
