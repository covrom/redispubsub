package redispubsub

import (
	"context"
	"fmt"
	"net/url"
	"path"
	"strconv"

	"github.com/go-redis/redis/v9"
	"gocloud.dev/pubsub"
)

// URLOpener opens Redis URLs like "redis://mytopic" for topics and
// "redis://group?topic=mytopic" for subscriptions.
//
// For topics, the URL's host+path is used as the topic name.
//
// For subscriptions, the URL's host+path is used as the group name,
// and the "topic" query parameter(s) are used as the set of topics to
// subscribe to. The "offset" parameter is available to subscribers to set
// the Redis Streams consumer's initial offset. Where "oldest" starts consuming from
// the oldest offset of the consumer group and "newest" starts consuming from
// the most recent offset on the topic.
type URLOpener struct {
	// Broker is the Redis parsed URL like "redis://<user>:<pass>@localhost:6379/<db>" with options.
	Broker *redis.Client

	// TopicOptions specifies the options to pass to OpenTopic.
	TopicOptions TopicOptions
	// SubscriptionOptions specifies the options to pass to OpenSubscription.
	SubscriptionOptions SubscriptionOptions
}

// OpenTopicURL opens a pubsub.Topic based on u.
func (o *URLOpener) OpenTopicURL(ctx context.Context, u *url.URL) (*pubsub.Topic, error) {
	opts := o.TopicOptions
	for param, value := range u.Query() {
		switch param {
		case "maxlen":
			vv, err := strconv.Atoi(value[0])
			if err != nil {
				return nil, fmt.Errorf("open topic %v: invalid query parameter %q: %w", u, param, err)
			}
			opts.MaxLen = int64(vv)
		default:
			return nil, fmt.Errorf("open topic %v: invalid query parameter %q", u, param)
		}
	}
	topicName := path.Join(u.Host, u.Path)
	return OpenTopic(o.Broker, topicName, &opts)
}

// OpenSubscriptionURL opens a pubsub.Subscription based on u.
func (o *URLOpener) OpenSubscriptionURL(ctx context.Context, u *url.URL) (*pubsub.Subscription, error) {
	var topic, consumer string
	var noack bool
	from := ""
	for param, value := range u.Query() {
		switch param {
		case "topic":
			topic = value[0]
		case "from":
			from = value[0]
		case "consumer":
			consumer = value[0]
		case "noack":
			noack = true
		default:
			return nil, fmt.Errorf("open subscription %v: invalid query parameter %q", u, param)
		}
	}
	if consumer == "" {
		return nil, fmt.Errorf("open subscription %v: undefined 'consumer' parameter", u)
	}
	o.SubscriptionOptions.From = from
	o.SubscriptionOptions.NoAck = noack
	group := path.Join(u.Host, u.Path)
	if group == "" {
		return nil, fmt.Errorf("open subscription %v: undefined host/path group name", u)
	}
	return OpenSubscription(o.Broker, group, topic, &o.SubscriptionOptions)
}
