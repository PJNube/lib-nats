package examples

import (
	"github.com/pjnube/lib-nats/pjnnats"
	"log"
	"testing"
	"time"
)

func TestStore(t *testing.T) {
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

	bucket := "bucket-abc"
	object := "obj"
	err = client.CreateObjectStore(conn.UUID, bucket, nil)
	if err != nil {
		log.Fatalf("Failed to create bucket: %v", err)
	}

	err = client.PutStoreObjectBytes(conn.UUID, bucket, object, []byte("hello world"), true)
	if err != nil {
		log.Fatalf("Failed to bucket object: %v", err)
	}

	data, err := client.GetObject(conn.UUID, bucket, object)
	if err != nil {
		log.Fatalf("Failed to get bucket's %s object %s: %v", bucket, object, err)
	}
	log.Println("Store object:", object, "data:", string(data))

	buckets, err := client.GetStores(conn.UUID)
	if err != nil {
		log.Fatalf("Failed to get buckets: %v", err)
	}
	log.Println("Stores:", buckets)

	objects, err := client.GetStoreObjects(conn.UUID, bucket)
	if err != nil {
		log.Fatalf("Failed to get %s bucket's object: %v", bucket, err)
	}
	log.Println("Store objects:", objects)

	err = client.DeleteObject(conn.UUID, bucket, object)
	if err != nil {
		log.Fatalf("Failed to delete object %s: %v", bucket, err)
	}

	err = client.DeleteObjectStore(conn.UUID, bucket)
	if err != nil {
		log.Fatalf("Failed to delete object bucket %s: %v", bucket, err)
	}

	buckets, err = client.GetStores(conn.UUID)
	if err != nil {
		log.Fatalf("Failed to get buckets: %v", err)
	}
	log.Println("Stores:", buckets)
}

// TestStoreZip uploads library-source.zip into NATS server and downloads it as library.zip
func TestStoreZip(t *testing.T) {
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

	bucket := "bucket-xyz"
	object := "library.zip"
	err = client.CreateObjectStore(conn.UUID, bucket, nil)
	if err != nil {
		log.Fatalf("Failed to create bucket: %v", err)
	}

	err = client.PutStoreObject(conn.UUID, bucket, object, "./library-source.zip", true)
	if err != nil {
		log.Fatalf("Failed to bucket object: %v", err)
	}

	err = client.DownloadObject(conn.UUID, bucket, object, "./", object)
	if err != nil {
		log.Fatalf("Failed to bucket object: %v", err)
	}

	err = client.DeleteObject(conn.UUID, bucket, object)
	if err != nil {
		log.Fatalf("Failed to delete object %s: %v", bucket, err)
	}

	err = client.DeleteObjectStore(conn.UUID, bucket)
	if err != nil {
		log.Fatalf("Failed to delete object bucket %s: %v", bucket, err)
	}

	buckets, err := client.GetStores(conn.UUID)
	if err != nil {
		log.Fatalf("Failed to get buckets: %v", err)
	}
	log.Println("Stores:", buckets)
}
