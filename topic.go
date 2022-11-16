package redispubsub

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/go-redis/redis/v9"
	"gocloud.dev/gcerrors"
	"gocloud.dev/pubsub"
	"gocloud.dev/pubsub/batcher"
	"gocloud.dev/pubsub/driver"
)

var sendBatcherOpts = &batcher.Options{
	MaxBatchSize: 100,
	MaxHandlers:  100, // max concurrency for sends
}

var errNotInitialized = errors.New("redispubsub: topic not initialized")

type topic struct {
	producer  *redis.Client
	topicName string
	opts      TopicOptions
}

// TopicOptions contains configuration options for topics.
type TopicOptions struct {
	// BatcherOptions adds constraints to the default batching done for sends.
	BatcherOptions batcher.Options
	MaxLen         int64
}

// OpenTopic creates a pubsub.Topic that sends to a Redis topic.
func OpenTopic(broker *redis.Client, topicName string, opts *TopicOptions) (*pubsub.Topic, error) {
	dt, err := openTopic(broker, topicName, opts)
	if err != nil {
		return nil, err
	}
	bo := sendBatcherOpts.NewMergedOptions(&opts.BatcherOptions)
	return pubsub.NewTopic(dt, bo), nil
}

// openTopic returns the driver for OpenTopic. This function exists so the test
// harness can get the driver interface implementation if it needs to.
func openTopic(broker *redis.Client, topicName string, opts *TopicOptions) (driver.Topic, error) {
	if opts == nil {
		opts = &TopicOptions{}
	}
	return &topic{producer: broker, topicName: topicName, opts: *opts}, nil
}

// SendBatch implements driver.Topic.SendBatch.
func (t *topic) SendBatch(ctx context.Context, dms []*driver.Message) error {
	if t == nil || t.producer == nil {
		return errNotInitialized
	}

	// Convert the messages to a slice of redis.XAddArgs.
	for _, dm := range dms {
		bm, err := json.Marshal(dm.Metadata)
		if err != nil {
			return err
		}

		msg := map[string]interface{}{
			"headers": bm,
			"body":    dm.Body,
		}
		args := &redis.XAddArgs{
			Stream: t.topicName,
			MaxLen: t.opts.MaxLen,
			Values: msg,
		}

		if dm.BeforeSend != nil {
			asFunc := func(i interface{}) bool {
				if p, ok := i.(**redis.XAddArgs); ok {
					*p = args
					return true
				}
				return false
			}
			if e := dm.BeforeSend(asFunc); e != nil {
				return e
			}
		}
		res, err := t.producer.XAdd(context.Background(), args).Result()
		if err != nil {
			return err
		}
		if dm.AfterSend != nil {
			asFunc := func(i interface{}) bool {
				if p, ok := i.(*string); ok {
					*p = res
					return true
				}
				return false
			}
			if err := dm.AfterSend(asFunc); err != nil {
				return err
			}
		}
	}
	return nil
}

// Close implements io.Closer.
func (t *topic) Close() error {
	return nil
}

// IsRetryable implements driver.Topic.IsRetryable.
func (t *topic) IsRetryable(error) bool {
	return false
}

// As implements driver.Topic.As.
func (t *topic) As(i interface{}) bool {
	if p, ok := i.(**redis.Client); ok {
		*p = t.producer
		return true
	}
	return false
}

// ErrorAs implements driver.Topic.ErrorAs.
func (t *topic) ErrorAs(err error, i interface{}) bool {
	return errorAs(err, i)
}

// ErrorCode implements driver.Topic.ErrorCode.
func (t *topic) ErrorCode(err error) gcerrors.ErrorCode {
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
