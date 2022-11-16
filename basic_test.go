package redispubsub_test

import (
	"context"
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

	err = topic.Send(ctx, &pubsub.Message{
		Body: []byte("Hello, World!\n"),
		// Metadata is optional and can be nil.
		Metadata: map[string]string{
			// These are examples of metadata.
			// There is nothing special about the key names.
			"language":   "en",
			"importance": "high",
		},
	})
	if err != nil {
		t.Error(err)
		return
	}
}
