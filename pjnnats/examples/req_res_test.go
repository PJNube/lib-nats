package examples

import (
	"fmt"
	"github.com/nats-io/nats.go"
	"github.com/pjnube/lib-nats/pjnnats"
	"log"
	"testing"
	"time"
)

func TestSubscribeWithResponse(t *testing.T) {
	client := pjnnats.New()

	opts := &pjnnats.NewOpts{
		URL:     "nats://localhost:4222", // Change this to your NATS server URL
		Timeout: 2 * time.Second,
	}

	conn, err := client.AddConnection(opts)
	if err != nil {
		log.Fatalf("Failed to add connection: %v", err)
	}

	subj := "example.request.response"

	msgHandler := func(msg *nats.Msg) *nats.Msg {
		authorization := msg.Header.Get("Authorization")
		fmt.Println("Authorization", authorization)
		fmt.Printf("Received message on subject '%s': %s\n", msg.Subject, string(msg.Data))
		msg.Data = append([]byte("Response: "), msg.Data...)
		return &nats.Msg{Data: msg.Data, Header: msg.Header}
	}

	err = client.SubscribeWithRespond(conn.UUID, subj, msgHandler)
	if err != nil {
		log.Fatalf("Failed to subscribe to subject: %v", err)
	}

	fmt.Printf("Subscribed to subject '%s'. Waiting for messages...\n", subj)

	// Keep the application running to listen for messages
	// (In a real application, use a more graceful shutdown method)
	select {}
}

func TestRequest(t *testing.T) {
	client := pjnnats.New()

	opts := &pjnnats.NewOpts{
		URL:     "nats://localhost:4222", // Change this to your NATS server URL
		Timeout: 2 * time.Second,
	}

	conn, err := client.AddConnection(opts)
	if err != nil {
		log.Fatalf("Failed to add connection: %v", err)
	}

	subj := "example.request.response"
	data := []byte(`{"message": "Hello, NATS!"}`)

	header := nats.Header{}
	header.Add("Authorization", "Bearer test")
	msg := nats.Msg{
		Subject: subj,
		Data:    data,
		Header:  header,
	}

	handler := func(msg *nats.Msg) {
		authorization := msg.Header.Get("Authorization")
		fmt.Println("Authorization", authorization)
		fmt.Printf("Received message on subject '%s': %s\n", msg.Subject, string(msg.Data))
	}

	err = client.Request(conn.UUID, msg, handler, nil)
	if err != nil {
		log.Fatalf("Failed to publish message: %v", err)
	}
	fmt.Println("Message published successfully")

	defer client.CloseConnection(conn.UUID)
}
