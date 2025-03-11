package examples

import (
	"github.com/PJNube/lib-nats/pjnnats"
	"github.com/stretchr/testify/assert"
	"log"
	"testing"
	"time"
)

func TestKVStore(t *testing.T) {
	client := pjnnats.New()

	opts := &pjnnats.NewOpts{
		URL:             "nats://localhost:4222", // Change this to your NATS server URL
		Timeout:         2 * time.Second,
		EnableJetStream: true,
	}

	conn, err := client.AddConnection(opts)
	if err != nil {
		log.Fatalf("Failed to add connection: %v", err)
	}

	bucket := "bucket-kv"
	key := "key1"
	value := "value1"
	err = client.CreateKVStore(conn.UUID, bucket)
	if err != nil {
		log.Fatalf("Failed to create store: %v", err)
	}

	_ = client.PutKV(conn.UUID, bucket, key, []byte(value))
	v, _ := client.GetKV(conn.UUID, bucket, key)
	assert.Equal(t, v, []byte(value))

	keys, _ := client.GetKVKeys(conn.UUID, bucket)
	assert.Contains(t, keys, key)

	keys, _ = client.GetKVKeysByPrefix(conn.UUID, bucket, "key")
	assert.Contains(t, keys, key)

	key2 := "key2"
	value2 := "value2"
	keys = []string{key, key2}
	_ = client.PutKV(conn.UUID, bucket, key2, []byte(value2))
	kvMap, _ := client.GetKVBulk(conn.UUID, bucket, keys)
	assert.Equal(t, kvMap[key2], []byte(value2))

	kvMap, _ = client.GetKVBulkByPrefix(conn.UUID, bucket, "key")
	assert.Equal(t, kvMap[key2], []byte(value2))

	_ = client.DeleteKV(conn.UUID, bucket, key)
	keys, _ = client.GetKVKeys(conn.UUID, bucket)
	assert.Len(t, keys, 1)

	_, _ = client.DeleteKVAll(conn.UUID, bucket)
	keys, _ = client.GetKVKeys(conn.UUID, bucket)
	assert.Len(t, keys, 0)

	_ = client.DeleteKVStore(conn.UUID, bucket)
}
