package messaging

type Message struct {
	SenderName    string `json:"sender_name"`
	RecipientName string `json:"recipient_name"`
	SentAt        int64  `json:"sent_at"`
	Payload       string `json:"payload"`
}
