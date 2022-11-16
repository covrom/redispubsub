package redispubsub_test

import (
	"bytes"
	"context"
	"sync"
	"testing"

	_ "github.com/covrom/redispubsub"
	"gocloud.dev/pubsub"
)

func TestBasicUsage(t *testing.T) {
	ctx := context.Background()
	topic, err := pubsub.OpenTopic(ctx, "redis://topics/1")
	if err != nil {
		t.Errorf("could not open topic: %v", err)
		return
	}
	defer topic.Shutdown(ctx)

	orig := &pubsub.Message{
		Body: []byte("Hello, World!\n"),
		// Metadata is optional and can be nil.
		Metadata: map[string]string{
			// These are examples of metadata.
			// There is nothing special about the key names.
			"language":   "en",
			"importance": "high",
		},
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		subs, err := pubsub.OpenSubscription(ctx, "redis://group1?consumer=cons1&topic=topics/1")
		if err != nil {
			t.Error(err)
			return
		}
		defer subs.Shutdown(ctx)
		for {
			msg, err := subs.Receive(ctx)
			if err != nil {
				// Errors from Receive indicate that Receive will no longer succeed.
				t.Errorf("Receiving message: %v", err)
				break
			}
			// Do work based on the message, for example:
			t.Logf("Got message: %q\n", msg.Body)
			// Messages must always be acknowledged with Ack.
			msg.Ack()

			if !bytes.Equal(msg.Body, orig.Body) {
				t.Error("body not equal")
			}
			break
		}
	}()

	err = topic.Send(ctx, orig)
	if err != nil {
		t.Error(err)
		return
	}

	wg.Wait()
}
