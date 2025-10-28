package pjnnats

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
)

func TestClient_Request_Concurrent(t *testing.T) {
	// Setup: Connect to NATS
	nc, err := nats.Connect(nats.DefaultURL)
	require.NoError(t, err)
	defer nc.Drain()

	client := New()
	client.AddConnection(&NewOpts{
		URL:             nats.DefaultURL,
		EnableJetStream: true,
	}, "local") // or however you manage connection UUIDs

	timeout := 10 * time.Second
	//   - local.ce.get.v0.nodes.path.add1
	//  - local.ce.get.v0.nodes.path.add2
	//  - local.ce.get.v0.nodes.path.addA
	//  - local.ce.get.v0.nodes.path.counter1
	//  - local.ce.get.v0.nodes.path.tktkA
	//  - local.ce.get.v0.nodes.path.tktkLoop
	//  - local.ce.get.v0.nodes.path.tktk1
	//  - local.ce.get.v0.nodes.path.stringConcatenate

	// Possible subjects
	subjects := []string{
		"add1",
		// "add2",
		"addA",
		"counter1",
		"tktkA",
		"tktkLoop",
		"tktk1",
		"stringConcatenate",
	}

	var wg sync.WaitGroup
	numRequests := 10

	wg.Add(numRequests)
	for i := 0; i < numRequests; i++ {
		suffix := subjects[rand.Intn(len(subjects))]
		msg := nats.Msg{
			Subject: "local.ce.patch.v0.nodes.path." + suffix,
			Data: []byte(`{
			"params":{},
			"body":{"metaData":{"position":{"x":920,"y":` + fmt.Sprintf("%d", i+233) + `}}}
		}`),
		}
		go func(i int) {
			defer wg.Done()
			err := client.Request("local", msg, func(m *nats.Msg) {
				fmt.Printf("Goroutine %02d got reply: %s\n", i, string(m.Data))
			}, &timeout)

			if err != nil {
				fmt.Printf("Goroutine %02d error: %v\n", i, err)
			} else {
				fmt.Printf("Goroutine %02d success\n", i)
			}
		}(i)
	}

	wg.Wait()
	fmt.Println("✅ All concurrent requests finished")
}
