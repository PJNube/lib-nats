package pjnnats

import (
	"encoding/json"
	"fmt"

	"github.com/nats-io/nats.go"
)

func (n *Client) PublishNotification(uuid, subject, event string, message any) error {
	payload, err := getNotifyPayload(message, event)
	if err != nil {
		return err
	}

	return n.Publish(uuid, nats.Msg{
		Subject: subject,
		Data:    payload,
	})
}

func getNotifyPayload(message any, event string) ([]byte, error) {
	payload, err := json.Marshal(map[string]any{
		"event": event,
		"data":  message,
	})
	if err != nil {
		return nil, fmt.Errorf("error marshalling notification payload: %w", err)
	}
	return payload, nil
}
