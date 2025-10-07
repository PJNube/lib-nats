package pjnnats

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/PJNube/lib-models/dtos"
	"github.com/nats-io/nats.go"
	"github.com/rs/xid"
)

type ConnectionStatus int

const (
	Active ConnectionStatus = iota
	Disabled
	NatsConnectionUUID = "local"
)

// Opts represents options for NATS operations.
type Opts struct {
	Timeout int // seconds
}

// Subjects represents a NATS subject with its type.
type Subjects struct {
	Type    string // "Subscribe" or "SubscribeWithRespond"
	Subject string
}

// NewOpts represents the options required for creating a new NATS connection.
type NewOpts struct {
	URL             string
	NatsConn        *nats.Conn
	Timeout         time.Duration
	EnableJetStream bool
	AuthToken       string      // For token-based authentication
	User            string      // For authentication
	Password        string      // For authentication
	CredsFile       string      // For NATS credentials file
	TLSConfig       *tls.Config // For TLS authentication
}

// Connection holds details about each NATS connection.
type Connection struct {
	UUID       string
	Connection *nats.Conn
	Status     ConnectionStatus
	Options    *NewOpts
	JSContext  nats.JetStreamContext
	Subjects   []*Subjects
}

// IClient is the extended interface with connection management and NATS methods.
type IClient interface {
	// Connection Management
	AddConnection(opts *NewOpts, optionalUUID ...string) (*Connection, error)
	GetConnection(uuid string) (*Connection, error)
	EditConnection(uuid string, opts *NewOpts) (*Connection, error)
	DeleteConnection(uuid string) error
	EnableConnection(uuid string) error
	DisableConnection(uuid string) error
	PingConnection(uuid string) error

	// NATS Methods
	Publish(uuid string, msg nats.Msg) error
	PublishNotification(subject, event string, message any) error
	Subscribe(uuid string, subject string, cb nats.MsgHandler) (*nats.Subscription, error)
	Request(uuid string, msg nats.Msg, cb nats.MsgHandler, duration *time.Duration) error
	SubscribeWithRespond(uuid string, subject string, handler func(msg *nats.Msg) *nats.Msg) error
	RequestAll(uuid string, subject string, data []byte, timeout *time.Duration) ([]*nats.Msg, error)
	CloseConnection(uuid string) error // Close a specific connection
	Close()                            // Close all connections

	// JetStream Object Store methods
	CreateObjectStore(uuid, bucket string, config *nats.ObjectStoreConfig) error
	PutStoreObject(uuid, bucket, name, filePath string, overwriteIfExisting bool) error
	PutStoreObjectBytes(uuid, bucket, name string, data []byte, overwriteIfExisting bool) error
	GetStoreObjects(uuid, bucket string) ([]*nats.ObjectInfo, error)
	GetStores(uuid string) ([]string, error)
	GetObject(uuid, bucket, name string) ([]byte, error)
	DownloadObject(uuid, bucket, name, destinationPath, destinationName string) error
	DeleteObject(uuid, bucket, name string) error
	DeleteObjectStore(uuid, bucket string) error

	// KV Store Methods
	CreateKVStore(uuid, bucket string) error
	PutKV(uuid string, bucket string, key string, value []byte) error
	GetKV(uuid string, bucket string, key string) ([]byte, error)
	GetKVKeys(uuid, bucket string) ([]string, error)
	GetKVKeysByPrefix(uuid, bucket, prefix string) ([]string, error)
	GetKVBulk(uuid string, bucket string, keys []string) (map[string][]byte, error)
	GetKVBulkByPrefix(uuid string, bucket string, prefix string) (map[string][]byte, error)
	DeleteKV(uuid string, bucket string, key string) error
	DeleteKVAll(uuid string, bucket string) (int, error)
	DeleteKVStore(uuid string, bucket string) error
}

// Client is the implementation of the IClient interface.
type Client struct {
	connections map[string]*Connection
	mu          sync.RWMutex
}

// New creates a new instance of IClient with an empty connection manager.
func New() *Client {
	return &Client{
		connections: make(map[string]*Connection),
	}
}

func (n *Client) GetConnection(uuid string) (*Connection, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	conn, exists := n.connections[uuid]
	if !exists {
		return nil, errors.New("connection not found")
	}
	return conn, nil
}

// AddConnection adds a new NATS connection and returns its UUID.
func (n *Client) AddConnection(opts *NewOpts, optionalUUID ...string) (*Connection, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	var uuid string
	if len(optionalUUID) > 0 {
		uuid = optionalUUID[0]
	} else {
		uuid = xid.New().String()
	}
	if opts.Timeout == 0 {
		opts.Timeout = 2
	}
	natsOptions := []nats.Option{
		nats.Timeout(opts.Timeout * time.Second),
	}

	if opts.AuthToken != "" {
		natsOptions = append(natsOptions, nats.Token(opts.AuthToken))
	}

	if opts.User != "" && opts.Password != "" {
		natsOptions = append(natsOptions, nats.UserInfo(opts.User, opts.Password))
	}

	if opts.CredsFile != "" {
		natsOptions = append(natsOptions, nats.UserCredentials(opts.CredsFile))
	}

	if opts.TLSConfig != nil {
		natsOptions = append(natsOptions, nats.Secure(opts.TLSConfig))
	}
	if opts.URL == "" {
		opts.URL = nats.DefaultURL
	}

	var nc *nats.Conn
	var err error
	if opts.NatsConn == nil {
		nc, err = nats.Connect(opts.URL, natsOptions...)
		if err != nil {
			return nil, fmt.Errorf("failed to connect to NATS: %w", err)
		}
	} else {
		nc = opts.NatsConn
	}

	var js nats.JetStreamContext
	if opts.EnableJetStream {
		js, err = nc.JetStream()
		if err != nil {
			nc.Close()
			return nil, fmt.Errorf("error initializing JetStream: %w", err)
		}
	}

	conn := &Connection{
		UUID:       uuid,
		Connection: nc,
		Status:     Active,
		Options:    opts,
		JSContext:  js,
	}

	n.connections[uuid] = conn

	return conn, nil
}

// EditConnection updates the connection parameters for a given UUID.
func (n *Client) EditConnection(uuid string, opts *NewOpts) (*Connection, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	conn, exists := n.connections[uuid]
	if !exists {
		return nil, errors.New("connection not found")
	}

	conn.Connection.Close()

	var nc *nats.Conn
	var err error
	if opts.NatsConn == nil {
		nc, err = nats.Connect(opts.URL, nats.Timeout(opts.Timeout*time.Second))
		if err != nil {
			return nil, fmt.Errorf("failed to connect to NATS: %w", err)
		}
	} else {
		nc = opts.NatsConn
	}

	var js nats.JetStreamContext
	if opts.EnableJetStream {
		js, err = nc.JetStream()
		if err != nil {
			nc.Close()
			return nil, fmt.Errorf("error initializing JetStream: %w", err)
		}
	}

	conn.Connection = nc
	conn.Options = opts
	conn.Status = Active
	conn.JSContext = js

	return conn, nil
}

// DeleteConnection removes and closes a connection by UUID.
func (n *Client) DeleteConnection(uuid string) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	conn, exists := n.connections[uuid]
	if !exists {
		return errors.New("connection not found")
	}

	conn.Connection.Close()
	delete(n.connections, uuid)

	return nil
}

// EnableConnection re-establishes a disabled connection.
func (n *Client) EnableConnection(uuid string) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	conn, exists := n.connections[uuid]
	if !exists {
		return errors.New("connection not found")
	}

	if conn.Status == Active {
		return errors.New("connection is already active")
	}

	var nc *nats.Conn
	var err error
	if conn.Options.NatsConn == nil {
		nc, err = nats.Connect(conn.Options.URL, nats.Timeout(time.Duration(conn.Options.Timeout)*time.Second))
		if err != nil {
			return fmt.Errorf("failed to reconnect to NATS: %w", err)
		}
	} else {
		nc = conn.Options.NatsConn
	}

	var js nats.JetStreamContext
	if conn.Options.EnableJetStream {
		js, err = nc.JetStream()
		if err != nil {
			nc.Close()
			return fmt.Errorf("error initializing JetStream: %w", err)
		}
	}

	conn.Connection = nc
	conn.JSContext = js
	conn.Status = Active

	return nil
}

// DisableConnection closes an active connection and marks it as disabled.
func (n *Client) DisableConnection(uuid string) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	conn, exists := n.connections[uuid]
	if !exists {
		return errors.New("connection not found")
	}

	if conn.Status == Disabled {
		return errors.New("connection is already disabled")
	}

	conn.Connection.Close()
	conn.Status = Disabled

	return nil
}

// PingConnection checks the health of a connection by flushing it.
func (n *Client) PingConnection(uuid string) error {
	conn, err := n.getActiveConnection(uuid)
	if err != nil {
		return err
	}

	if !conn.Connection.IsConnected() {
		return errors.New("connection is not active")
	}

	if err := conn.Connection.FlushTimeout(5 * time.Second); err != nil {
		return fmt.Errorf("ping failed: %w", err)
	}

	return nil
}

// CloseConnection closes a specific connection by UUID.
func (n *Client) CloseConnection(uuid string) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	conn, exists := n.connections[uuid]
	if !exists {
		return errors.New("connection not found")
	}

	conn.Connection.Close()
	delete(n.connections, uuid)

	return nil
}

// Close closes all active connections.
func (n *Client) Close() {
	n.mu.Lock()
	defer n.mu.Unlock()

	for uuid, conn := range n.connections {
		conn.Connection.Close()
		delete(n.connections, uuid)
	}
}

// Helper method to retrieve an active connection by UUID.
func (n *Client) getActiveConnection(uuid string) (*Connection, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	conn, exists := n.connections[uuid]
	if !exists {
		return nil, errors.New("connection not found")
	}
	if conn.Status != Active || !conn.Connection.IsConnected() {
		return nil, errors.New("connection is not active")
	}
	return conn, nil
}

func (n *Client) Publish(uuid string, msg nats.Msg) error {
	conn, err := n.getActiveConnection(uuid)
	if err != nil {
		return err
	}
	return conn.Connection.PublishMsg(&msg)
}

func (n *Client) PublishNotification(subject, event string, message any) error {
	payload, err := getNotifyPayload(message, event)
	if err != nil {
		return err
	}

	return n.Publish(NatsConnectionUUID, nats.Msg{
		Subject: subject,
		Data:    payload,
	})
}

func getNotifyPayload(message any, event string) ([]byte, error) {
	notifyPayload := &dtos.NotificationPayload{
		Event: event,
		Data:  message,
	}

	payload, err := json.Marshal(notifyPayload)
	if err != nil {
		return nil, fmt.Errorf("error marshalling notification payload: %w", err)
	}

	return payload, nil
}

// Subscribe listens for messages on a specific connection.
func (n *Client) Subscribe(uuid string, subject string, cb nats.MsgHandler) (*nats.Subscription, error) {
	conn, err := n.getActiveConnection(uuid)
	if err != nil {
		return nil, err
	}

	conn.Subjects = append(conn.Subjects, &Subjects{
		Type:    "Subscribe",
		Subject: subject,
	})

	return conn.Connection.Subscribe(subject, cb)
}

// Request sends a message on a specific connection. Received subject example: _INBOX.Z1ObtuQxY6Mnls2jjwO4e1.EAMcteWJ
func (n *Client) Request(uuid string, msg nats.Msg, cb nats.MsgHandler, timeout *time.Duration) error {
	conn, err := n.getActiveConnection(uuid)
	if err != nil {
		return err
	}

	if timeout == nil {
		timeout = &conn.Options.Timeout
	}

	m, err := conn.Connection.RequestMsg(&msg, *timeout)
	if err != nil {
		return err
	}

	if cb != nil {
		cb(m)
	}
	return nil
}

func (n *Client) SynchronousRequest(uuid string, msg nats.Msg, timeout *time.Duration) (*nats.Msg, error) {
	msgChan := make(chan *nats.Msg, 1)
	handler := func(msg *nats.Msg) {
		msgChan <- msg
	}
	err := n.Request(uuid, msg, handler, timeout)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	select {
	case <-ctx.Done():
		return nil, errors.New("request timeout")
	case data := <-msgChan:
		return data, nil
	}
}

func (n *Client) SubscribeWithRespond(uuid, subj string, handler func(msg *nats.Msg) *nats.Msg) error {
	conn, err := n.getActiveConnection(uuid)
	if err != nil {
		return err
	}

	conn.Subjects = append(conn.Subjects, &Subjects{
		Type:    "SubscribeWithRespond",
		Subject: subj,
	})

	_, err = conn.Connection.Subscribe(subj, func(msg *nats.Msg) {
		// Process each message in its own goroutine for concurrency
		go func(m *nats.Msg) {
			responseMsg := handler(m)
			if err := m.RespondMsg(responseMsg); err != nil {
				return
			}
		}(msg)
	})

	return err
}

// RequestAll sends a request and collects all responses on a specific connection.
func (n *Client) RequestAll(uuid string, subj string, data []byte, timeout *time.Duration) ([]*nats.Msg, error) {
	conn, err := n.getActiveConnection(uuid)
	if err != nil {
		return nil, err
	}

	if timeout == nil {
		timeout = &conn.Options.Timeout
	}

	inbox := nats.NewInbox()
	sub, err := conn.Connection.SubscribeSync(inbox)
	if err != nil {
		return nil, err
	}
	defer sub.Unsubscribe()

	m := &nats.Msg{
		Subject: subj,
		Reply:   inbox,
		Data:    data,
	}
	err = conn.Connection.PublishMsg(m)
	if err != nil {
		return nil, err
	}

	var responses []*nats.Msg
	deadline := time.Now().Add(*timeout)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}
		msg, err := sub.NextMsg(remaining)
		if err != nil {
			if errors.Is(err, nats.ErrTimeout) {
				break
			}
			return nil, err
		}
		responses = append(responses, msg)
	}

	return responses, nil
}

// CreateObjectStore creates an object store on a specific connection.
func (n *Client) CreateObjectStore(uuid, bucket string, config *nats.ObjectStoreConfig) error {
	conn, err := n.getActiveConnection(uuid)
	if err != nil {
		return err
	}

	_, err = conn.JSContext.ObjectStore(bucket)
	if err != nil {
		if config == nil {
			config = &nats.ObjectStoreConfig{
				Bucket: bucket,
			}
		}
		_, err = conn.JSContext.CreateObjectStore(config)
		if err != nil {
			return err
		}
	}
	return nil
}

// PutStoreObject creates a new object in the object store on a specific connection.
func (n *Client) PutStoreObject(uuid, bucket, name, filePath string, overwriteIfExisting bool) error {
	conn, err := n.getActiveConnection(uuid)
	if err != nil {
		return err
	}

	store, err := conn.JSContext.ObjectStore(bucket)
	if err != nil {
		return err
	}

	obj, err := store.Get(name)
	if err == nil {
		obj.Close()
		if overwriteIfExisting {
			err = store.Delete(name)
			if err != nil {
				return err
			}
		} else {
			return nil
		}
	}

	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = store.Put(&nats.ObjectMeta{Name: name}, file)
	if err != nil {
		return err
	}
	return nil
}

// PutStoreObjectBytes adds a new object (as bytes) to the object store on a specific connection.
func (n *Client) PutStoreObjectBytes(uuid, bucket, name string, data []byte, overwriteIfExisting bool) error {
	conn, err := n.getActiveConnection(uuid)
	if err != nil {
		return err
	}

	store, err := conn.JSContext.ObjectStore(bucket)
	if err != nil {
		return err
	}

	obj, err := store.Get(name)
	if err == nil {
		obj.Close()
		if overwriteIfExisting {
			err = store.Delete(name)
			if err != nil {
				return err
			}
		} else {
			return nil
		}
	}

	_, err = store.PutBytes(name, data)
	if err != nil {
		return err
	}

	return nil
}

// GetStoreObjects retrieves details for all objects in the specified object store on a specific connection.
func (n *Client) GetStoreObjects(uuid, bucket string) ([]*nats.ObjectInfo, error) {
	conn, err := n.getActiveConnection(uuid)
	if err != nil {
		return nil, err
	}

	store, err := conn.JSContext.ObjectStore(bucket)
	if err != nil {
		return nil, err
	}

	return store.List()
}

// GetStores returns the list of available object store names on a specific connection.
func (n *Client) GetStores(uuid string) ([]string, error) {
	conn, err := n.getActiveConnection(uuid)
	if err != nil {
		return nil, err
	}

	bucketsChan := conn.JSContext.ObjectStoreNames()
	var stores []string
	for bucket := range bucketsChan {
		stores = append(stores, bucket)
	}
	return stores, nil
}

// GetObject retrieves an object by name from the object store on a specific connection.
func (n *Client) GetObject(uuid, bucket, name string) ([]byte, error) {
	conn, err := n.getActiveConnection(uuid)
	if err != nil {
		return nil, err
	}

	store, err := conn.JSContext.ObjectStore(bucket)
	if err != nil {
		return nil, err
	}

	obj, err := store.Get(name)
	if err != nil {
		return nil, err
	}
	defer obj.Close()

	data, err := io.ReadAll(obj)
	if err != nil {
		return nil, err
	}

	return data, nil
}

// DownloadObject downloads an object from the object store and saves it to the specified destination directory on a specific connection.
func (n *Client) DownloadObject(uuid, bucket, name, destinationPath, destinationName string) error {
	conn, err := n.getActiveConnection(uuid)
	if err != nil {
		return err
	}

	store, err := conn.JSContext.ObjectStore(bucket)
	if err != nil {
		return err
	}

	obj, err := store.Get(name)
	if err != nil {
		return err
	}
	defer obj.Close()

	destInfo, err := os.Stat(destinationPath)
	if err != nil {
		if os.IsNotExist(err) {
			err = os.MkdirAll(destinationPath, os.ModePerm)
			if err != nil {
				return err
			}
		} else {
			return err
		}
	} else if !destInfo.IsDir() {
		return fmt.Errorf("destination path %s is not a directory", destinationPath)
	}

	destinationFilePath := filepath.Join(destinationPath, destinationName)

	outFile, err := os.Create(destinationFilePath)
	if err != nil {
		return err
	}
	defer outFile.Close()

	_, err = io.Copy(outFile, obj)
	if err != nil {
		return err
	}

	return nil
}

// DeleteObject removes an object from the object store by name on a specific connection.
func (n *Client) DeleteObject(uuid, bucket, name string) error {
	conn, err := n.getActiveConnection(uuid)
	if err != nil {
		return err
	}

	store, err := conn.JSContext.ObjectStore(bucket)
	if err != nil {
		return err
	}

	err = store.Delete(name)
	if err != nil {
		return err
	}
	return nil
}

// DeleteObjectStore deletes the entire object store by name on a specific connection.
func (n *Client) DeleteObjectStore(uuid, bucket string) error {
	conn, err := n.getActiveConnection(uuid)
	if err != nil {
		return err
	}

	err = conn.JSContext.DeleteObjectStore(bucket)
	if err != nil {
		return err
	}
	return nil
}

// CreateKVStore creates a new Key-Value store on a specific connection.
func (n *Client) CreateKVStore(uuid, bucket string) error {
	conn, err := n.getActiveConnection(uuid)
	if err != nil {
		return err
	}

	store, err := conn.JSContext.KeyValue(bucket)
	if err != nil {
		if errors.Is(err, nats.ErrBucketNotFound) {
		} else {
			return err
		}
	}

	if store != nil { // store already exists
		return nil
	}

	_, err = conn.JSContext.CreateKeyValue(&nats.KeyValueConfig{
		Bucket: bucket,
	})
	if err != nil {
		return fmt.Errorf("error creating KV store: %v", err)
	}

	return nil
}

// PutKV creates or updates a key-value pair in the store on a specific connection.
func (n *Client) PutKV(uuid string, bucket string, key string, value []byte) error {
	conn, err := n.getActiveConnection(uuid)
	if err != nil {
		return err
	}

	kv, err := conn.JSContext.KeyValue(bucket)
	if err != nil {
		return fmt.Errorf("error getting KV store: %v", err)
	}
	_, err = kv.Put(key, value)
	if err != nil {
		return err
	}

	return nil
}

// GetKV retrieves a value from the KV store by its key on a specific connection.
func (n *Client) GetKV(uuid string, bucket string, key string) ([]byte, error) {
	conn, err := n.getActiveConnection(uuid)
	if err != nil {
		return nil, err
	}

	kv, err := conn.JSContext.KeyValue(bucket)
	if err != nil {
		return nil, fmt.Errorf("error getting KV store: %v", err)
	}

	entry, err := kv.Get(key)
	if err != nil {
		return nil, err
	}

	return entry.Value(), nil
}

// GetKVBulk retrieves the values for a list of keys from the Key-Value store.
func (n *Client) GetKVBulk(uuid string, bucket string, keys []string) (map[string][]byte, error) {
	conn, err := n.getActiveConnection(uuid)
	if err != nil {
		return nil, err
	}

	kv, err := conn.JSContext.KeyValue(bucket)
	if err != nil {
		return nil, fmt.Errorf("error getting KV store: %v", err)
	}

	results := make(map[string][]byte)
	for _, key := range keys {
		entry, err := kv.Get(key)
		if err != nil {
			// Handle missing keys by skipping them or logging if necessary
			if errors.Is(err, nats.ErrKeyNotFound) {
				results[key] = nil // Set to nil if the key doesn't exist
				continue
			}
			return nil, err
		}

		results[key] = entry.Value()
	}

	return results, nil
}

// GetKVBulkByPrefix retrieves the values for a list of keys from the Key-Value store.
func (n *Client) GetKVBulkByPrefix(uuid string, bucket, prefix string) (map[string][]byte, error) {
	keys, err := n.GetKVKeysByPrefix(uuid, bucket, prefix)
	if err != nil {
		return nil, err
	}
	return n.GetKVBulk(uuid, bucket, keys)
}

func (n *Client) GetKVKeys(uuid, bucket string) ([]string, error) {
	conn, err := n.getActiveConnection(uuid)
	if err != nil {
		return nil, err
	}

	kv, err := conn.JSContext.KeyValue(bucket)
	if err != nil {
		return nil, fmt.Errorf("error getting KV store: %v", err)
	}
	return kv.Keys()
}

func (n *Client) GetKVKeysByPrefix(uuid, bucket, prefix string) ([]string, error) {
	conn, err := n.getActiveConnection(uuid)
	if err != nil {
		return nil, err
	}

	kv, err := conn.JSContext.KeyValue(bucket)
	if err != nil {
		return nil, fmt.Errorf("error getting KV store: %v", err)
	}
	keys, err := kv.Keys()
	if err != nil {
		return nil, err
	}
	var out []string
	for _, key := range keys {
		if strings.HasPrefix(key, prefix) {
			out = append(out, key)
		}
	}
	return out, nil
}

// DeleteKV deletes a key-value pair from the store by its key on a specific connection.
func (n *Client) DeleteKV(uuid string, bucket string, key string) error {
	conn, err := n.getActiveConnection(uuid)
	if err != nil {
		return err
	}

	kv, err := conn.JSContext.KeyValue(bucket)
	if err != nil {
		return fmt.Errorf("error getting KV store: %v", err)
	}

	err = kv.Delete(key)
	if err != nil {
		return err
	}

	return nil
}

func (n *Client) DeleteKVAll(uuid string, bucket string) (int, error) {
	conn, err := n.getActiveConnection(uuid)
	if err != nil {
		return 0, err
	}

	kv, err := conn.JSContext.KeyValue(bucket)
	if err != nil {
		return 0, fmt.Errorf("error getting KV store: %v", err)
	}

	keys, err := kv.Keys()
	if err != nil {
		return 0, err
	}
	var count int
	for _, key := range keys {
		count++
		n.DeleteKV(uuid, bucket, key)
	}

	return count, nil
}

// DeleteKVStore deletes an entire Key-Value store by its name on a specific connection.
func (n *Client) DeleteKVStore(uuid string, bucket string) error {
	conn, err := n.getActiveConnection(uuid)
	if err != nil {
		return err
	}

	err = conn.JSContext.DeleteKeyValue(bucket)
	if err != nil {
		return err
	}

	return nil
}
