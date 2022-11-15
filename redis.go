// Package redispubsub provides an implementation of pubsub for Redis.
// It requires a minimum Redis version of 6.x for Streams support.
//
// redispubsub does not support Message.Nack; Message.Nackable will return
// false, and Message.Nack will panic if called.
//
// # URLs
//
// For pubsub.OpenTopic and pubsub.OpenSubscription, redispubsub registers
// for the scheme "redis".
// The default URL opener will connect to a Redis Server based
// on the environment variable "REDIS_URL", expected to
// server address like "redis://<user>:<pass>@localhost:6379/<db>".
// To customize the URL opener, or for more details on the URL format,
// see URLOpener.
// See https://gocloud.dev/concepts/urls/ for background information.
//
// # Escaping
//
// Go CDK supports all UTF-8 strings. No escaping is required for Redis.
package redispubsub

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/Shopify/sarama"
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

var recvBatcherOpts = &batcher.Options{
	// Concurrency doesn't make sense here.
	MaxBatchSize: 1,
	MaxHandlers:  1,
}

func init() {
	opener := new(defaultOpener)
	pubsub.DefaultURLMux().RegisterTopic(Scheme, opener)
	pubsub.DefaultURLMux().RegisterSubscription(Scheme, opener)
}

// Scheme is the URL scheme that redispubsub registers its URLOpeners under on pubsub.DefaultMux.
const Scheme = "redis"

type subscription struct {
	opts          SubscriptionOptions
	closeCh       chan struct{} // closed when we've shut down
	joinCh        chan struct{} // closed when we join for the first time
	cancel        func()        // cancels the background consumer
	closeErr      error         // fatal error detected by the background consumer
	consumerGroup sarama.ConsumerGroup

	mu             sync.Mutex
	unacked        []*ackInfo
	sess           sarama.ConsumerGroupSession // current session, if any, used for marking offset updates
	expectedClaims int                         // # of expected claims for the current session, they should be added via ConsumeClaim
	claims         []sarama.ConsumerGroupClaim // claims in the current session
}

// ackInfo stores info about a message and whether it has been acked.
// It is used as the driver.AckID.
type ackInfo struct {
	msg   *sarama.ConsumerMessage
	acked bool
}

// SubscriptionOptions contains configuration for subscriptions.
type SubscriptionOptions struct {
	// KeyName optionally sets the Message.Metadata key in which to store the
	// Kafka message key. If set, and if the Kafka message key is non-empty,
	// the key value will be stored in Message.Metadata under KeyName.
	KeyName string

	// WaitForJoin causes OpenSubscription to wait for up to WaitForJoin
	// to allow the client to join the consumer group.
	// Messages sent to the topic before the client joins the group
	// may not be received by this subscription.
	// OpenSubscription will succeed even if WaitForJoin elapses and
	// the subscription still hasn't been joined successfully.
	WaitForJoin time.Duration
}

// OpenSubscription creates a pubsub.Subscription that joins group, receiving
// messages from topics.
//
// It uses a sarama.ConsumerGroup to receive messages. Consumer options can
// be configured in the Consumer section of the sarama.Config:
// https://godoc.org/github.com/Shopify/sarama#Config.
func OpenSubscription(broker *redis.Client, group string, topics []string, opts *SubscriptionOptions) (*pubsub.Subscription, error) {
	ds, err := openSubscription(broker, group, topics, opts)
	if err != nil {
		return nil, err
	}
	return pubsub.NewSubscription(ds, recvBatcherOpts, nil), nil
}

// openSubscription returns the driver for OpenSubscription. This function
// exists so the test harness can get the driver interface implementation if it
// needs to.
func openSubscription(broker *redis.Client, group string, topics []string, opts *SubscriptionOptions) (driver.Subscription, error) {
	if opts == nil {
		opts = &SubscriptionOptions{}
	}
	consumerGroup, err := sarama.NewConsumerGroup(brokers, group, config)
	if err != nil {
		return nil, err
	}
	// Create a cancelable context for the background goroutine that
	// consumes messages.
	ctx, cancel := context.WithCancel(context.Background())
	joinCh := make(chan struct{})
	ds := &subscription{
		opts:          *opts,
		consumerGroup: consumerGroup,
		closeCh:       make(chan struct{}),
		joinCh:        joinCh,
		cancel:        cancel,
	}
	// Start a background consumer. It should run until ctx is cancelled
	// by Close, or until there's a fatal error (e.g., topic doesn't exist).
	// We're registering ds as our ConsumerGroupHandler, so sarama will
	// call [Setup, ConsumeClaim (possibly more than once), Cleanup]
	// repeatedly as the consumer group is rebalanced.
	// See https://godoc.org/github.com/Shopify/sarama#ConsumerGroup.
	go func() {
		for {
			ds.closeErr = consumerGroup.Consume(ctx, topics, ds)
			if ds.closeErr != nil || ctx.Err() != nil {
				consumerGroup.Close()
				close(ds.closeCh)
				break
			}
		}
	}()
	if opts.WaitForJoin > 0 {
		// Best effort wait for first consumer group session.
		select {
		case <-joinCh:
		case <-ds.closeCh:
		case <-time.After(opts.WaitForJoin):
		}
	}
	return ds, nil
}

// Setup implements sarama.ConsumerGroupHandler.Setup. It is called whenever
// a new session with the broker is starting.
func (s *subscription) Setup(sess sarama.ConsumerGroupSession) error {
	// Record the current session.
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sess = sess
	s.expectedClaims = 0
	for _, claims := range sess.Claims() {
		s.expectedClaims += len(claims)
	}
	return nil
}

// Cleanup implements sarama.ConsumerGroupHandler.Cleanup.
func (s *subscription) Cleanup(sarama.ConsumerGroupSession) error {
	// Clear the current session.
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sess = nil
	s.expectedClaims = 0
	s.claims = nil
	return nil
}

// ConsumeClaim implements sarama.ConsumerGroupHandler.ConsumeClaim.
// This is where messages are actually delivered, via a channel.
func (s *subscription) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	s.mu.Lock()
	s.claims = append(s.claims, claim)
	// Once all of the expected claims have registered, close joinCh to (possibly) wake up OpenSubscription.
	if s.joinCh != nil && len(s.claims) == s.expectedClaims {
		close(s.joinCh)
		s.joinCh = nil
	}
	s.mu.Unlock()
	<-sess.Context().Done()
	return nil
}

// ReceiveBatch implements driver.Subscription.ReceiveBatch.
func (s *subscription) ReceiveBatch(ctx context.Context, maxMessages int) ([]*driver.Message, error) {
	// Try to read maxMessages for up to 100ms before giving up.
	maxWaitCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()

	for {
		// We'll give up after maxWaitCtx is Done, or if s.closeCh is closed.
		// Otherwise, we want to pull a message from one of the channels in the
		// claim(s) we've been given.
		//
		// Note: we could multiplex this by ranging over each claim.Messages(),
		// writing the messages to a single ch, and then reading from that ch
		// here. However, this results in us reading messages from Kafka and
		// essentially queueing them here; when the session is closed for whatever
		// reason, those messages are lost, which may or may not be an issue
		// depending on the Kafka configuration being used.
		//
		// It seems safer to use reflect.Select to explicitly only get a single
		// message at a time, and hand it directly to the user.
		//
		// reflect.Select is essentially a "select" statement, but allows us to
		// build the cases dynamically. We need that because we need a case for
		// each of the claims in s.claims.
		s.mu.Lock()
		cases := make([]reflect.SelectCase, 0, len(s.claims)+2)
		// Add a case for s.closeCh being closed, at index = 0.
		cases = append(cases, reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(s.closeCh),
		})
		// Add a case for maxWaitCtx being Done, at index = 1.
		cases = append(cases, reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(maxWaitCtx.Done()),
		})
		// Add a case per claim, reading from the claim's Messages channel.
		for _, claim := range s.claims {
			cases = append(cases, reflect.SelectCase{
				Dir:  reflect.SelectRecv,
				Chan: reflect.ValueOf(claim.Messages()),
			})
		}
		s.mu.Unlock()
		i, v, ok := reflect.Select(cases)
		if !ok {
			// The i'th channel was closed.
			switch i {
			case 0: // s.closeCh
				return nil, s.closeErr
			case 1: // maxWaitCtx
				// We've tried for a while to get a message, but didn't get any.
				// Return an empty slice; the portable type will call us back.
				return nil, ctx.Err()
			}
			// Otherwise, if one of the claim channels closed, we're probably ending
			// a session. Just keep trying.
			continue
		}
		msg := v.Interface().(*sarama.ConsumerMessage)

		// We've got a message! It should not be nil.
		// Read the metadata from msg.Headers.
		md := map[string]string{}
		for _, h := range msg.Headers {
			md[string(h.Key)] = string(h.Value)
		}
		// Add a metadata entry for the message key if appropriate.
		if len(msg.Key) > 0 && s.opts.KeyName != "" {
			md[s.opts.KeyName] = string(msg.Key)
		}
		ack := &ackInfo{msg: msg}
		var loggableID string
		if len(msg.Key) == 0 {
			loggableID = fmt.Sprintf("partition %d offset %d", msg.Partition, msg.Offset)
		} else {
			loggableID = string(msg.Key)
		}
		dm := &driver.Message{
			LoggableID: loggableID,
			Body:       msg.Value,
			Metadata:   md,
			AckID:      ack,
			AsFunc: func(i interface{}) bool {
				if p, ok := i.(**sarama.ConsumerMessage); ok {
					*p = msg
					return true
				}
				return false
			},
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		s.unacked = append(s.unacked, ack)
		return []*driver.Message{dm}, nil
	}
}

// SendAcks implements driver.Subscription.SendAcks.
func (s *subscription) SendAcks(ctx context.Context, ids []driver.AckID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Mark them all acked.
	for _, id := range ids {
		id.(*ackInfo).acked = true
	}
	if s.sess == nil {
		// We don't have a current session, so we can't send offset updates.
		// We'll just wait until next time and retry.
		return nil
	}
	// Mark all of the acked messages at the head of the slice. Since Kafka only
	// stores a single offset, we can't mark messages that aren't at the head; that
	// would move the offset past other as-yet-unacked messages.
	for len(s.unacked) > 0 && s.unacked[0].acked {
		s.sess.MarkMessage(s.unacked[0].msg, "")
		s.unacked = s.unacked[1:]
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
	// Cancel the ctx for the background goroutine and wait until it's done.
	s.cancel()
	<-s.closeCh
	return nil
}

// IsRetryable implements driver.Subscription.IsRetryable.
func (*subscription) IsRetryable(error) bool {
	return false
}

// As implements driver.Subscription.As.
func (s *subscription) As(i interface{}) bool {
	if p, ok := i.(*sarama.ConsumerGroup); ok {
		*p = s.consumerGroup
		return true
	}
	if p, ok := i.(*sarama.ConsumerGroupSession); ok {
		s.mu.Lock()
		defer s.mu.Unlock()
		*p = s.sess
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

func errorAs(err error, i interface{}) bool {
	switch terr := err.(type) {
	case redis.Error:
		if p, ok := i.(*redis.Error); ok {
			*p = terr
			return true
		}
	}
	return false
}
