package redispubsub

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/go-redis/redis/v9"
	"gocloud.dev/gcerrors"
	"gocloud.dev/pubsub"
	"gocloud.dev/pubsub/batcher"
	"gocloud.dev/pubsub/driver"
)

var recvBatcherOpts = &batcher.Options{
	// Concurrency doesn't make sense here.
	MaxBatchSize: 1,
	MaxHandlers:  1,
}

type subscription struct {
	broker *redis.Client
	group  string
	topic  string
	opts   SubscriptionOptions
	args   *redis.XReadGroupArgs
}

// SubscriptionOptions contains configuration for subscriptions.
type SubscriptionOptions struct {
	From     string // starting id, $ by default
	Consumer string // unique consumer name
	NoAck    bool
}

// OpenSubscription creates a pubsub.Subscription that joins group, receiving
// messages from topics.
func OpenSubscription(broker *redis.Client, group, topic string, opts *SubscriptionOptions) (*pubsub.Subscription, error) {
	ds, err := openSubscription(broker, group, topic, opts)
	if err != nil {
		return nil, err
	}
	return pubsub.NewSubscription(ds, recvBatcherOpts, nil), nil
}

// openSubscription returns the driver for OpenSubscription. This function
// exists so the test harness can get the driver interface implementation if it
// needs to.
func openSubscription(broker *redis.Client, group, topic string, opts *SubscriptionOptions) (driver.Subscription, error) {
	if opts == nil {
		opts = &SubscriptionOptions{}
	}
	if opts.From == "" {
		opts.From = "$"
	}
	// Create a consumer group eater on the stream, and start consuming from
	// the latest message (represented by $) or From id
	_, err := broker.XGroupCreateMkStream(context.Background(), topic, group, opts.From).Result()
	if err != nil && !strings.HasPrefix(err.Error(), "BUSYGROUP") {
		return nil, err
	}

	// Read messages in the consumer group eater that have not been read by other consumers>
	// Will block after running, name the input on the redis client  XADD "example:stream" * foodId 1003 foodName Coca-Cola will get the result
	xReadGroupArgs := &redis.XReadGroupArgs{
		Group:    group,                // consumer group
		Consumer: opts.Consumer,        // Consumer, created on-the-fly
		Streams:  []string{topic, ">"}, // stream
		Block:    0,                    // infinite waiting
		NoAck:    opts.NoAck,           // Confirmation required
	}

	ds := &subscription{
		broker: broker,
		opts:   *opts,
		args:   xReadGroupArgs,
		group:  group,
		topic:  topic,
	}
	return ds, nil
}

// ReceiveBatch implements driver.Subscription.ReceiveBatch.
func (s *subscription) ReceiveBatch(ctx context.Context, maxMessages int) ([]*driver.Message, error) {
	args := *s.args
	// if maxMessages > 0 {
	// 	args.Count = int64(maxMessages)
	// }

	// What will happen if we crash in the middle of processing messages,
	// is that our messages will remain in the pending entries list,
	// so we can access our history by giving XREADGROUP initially an ID of 0,
	// and performing the same loop. Once providing an ID of 0 the reply
	// is an empty set of messages, we know that we processed and acknowledged
	// all the pending messages.
	args0 := args
	args0.Streams = []string{args.Streams[0], "0"}
	if dm, err := s.receiveNextMessage(ctx, &args0); dm != nil && err == nil {
		return []*driver.Message{dm}, nil
	}

	// We can start to use > as ID, in order to get the new messages
	// and rejoin the consumers that are processing new things.
	dm, err := s.receiveNextMessage(ctx, &args)
	if err != nil {
		return nil, err
	}
	return []*driver.Message{dm}, nil
}

func (s *subscription) receiveNextMessage(ctx context.Context, args *redis.XReadGroupArgs) (*driver.Message, error) {
	xStreamSlice, err := s.broker.XReadGroup(ctx, args).Result()
	if err != nil || ctx.Err() != nil {
		if err == nil {
			err = ctx.Err()
		}
		return nil, err
	}
	if len(xStreamSlice) == 0 || len(xStreamSlice[0].Messages) == 0 {
		return nil, nil
	}
	msg := xStreamSlice[0].Messages[0]
	bd := []byte(msg.Values["body"].(string))
	var bm map[string]string
	if err := json.Unmarshal([]byte(msg.Values["headers"].(string)), &bm); err != nil {
		return nil, err
	}

	return &driver.Message{
		LoggableID: fmt.Sprintf("msg %s", msg.ID),
		Body:       bd,
		Metadata:   bm,
		AckID:      msg.ID,
		AsFunc: func(i interface{}) bool {
			if p, ok := i.(*redis.XMessage); ok {
				*p = msg
				return true
			}
			return false
		},
	}, nil
}

// SendAcks implements driver.Subscription.SendAcks.
func (s *subscription) SendAcks(ctx context.Context, ids []driver.AckID) error {
	// Mark them all acked.
	for _, id := range ids {
		_, err := s.broker.XAck(ctx, s.topic, s.group, fmt.Sprint(id)).Result()
		if err != nil || ctx.Err() != nil {
			if err == nil {
				err = ctx.Err()
			}
			return fmt.Errorf("ack id %s error: %w", id, err)
		}
	}
	return nil
}

// CanNack implements driver.CanNack.
func (s *subscription) CanNack() bool {
	// Nacking a single message doesn't make sense with the way Kafka maintains
	// offsets.
	return false
}

// SendNacks implements driver.Subscription.SendNacks.
func (s *subscription) SendNacks(ctx context.Context, ids []driver.AckID) error {
	panic("unreachable")
}

// Close implements io.Closer.
func (s *subscription) Close() error {
	return nil
}

// IsRetryable implements driver.Subscription.IsRetryable.
func (*subscription) IsRetryable(error) bool {
	return false
}

// As implements driver.Subscription.As.
func (s *subscription) As(i interface{}) bool {
	if p, ok := i.(*redis.XReadGroupArgs); ok {
		*p = *s.args
		return true
	}
	return false
}

// ErrorAs implements driver.Subscription.ErrorAs.
func (s *subscription) ErrorAs(err error, i interface{}) bool {
	return errorAs(err, i)
}

// ErrorCode implements driver.Subscription.ErrorCode.
func (*subscription) ErrorCode(err error) gcerrors.ErrorCode {
	switch err {
	case nil:
		return gcerrors.OK
	case context.Canceled:
		return gcerrors.Canceled
	case errNotInitialized:
		return gcerrors.NotFound
	}
	return gcerrors.Unknown
}
