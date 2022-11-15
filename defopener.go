package redispubsub

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"sync"

	"github.com/go-redis/redis/v9"
	"gocloud.dev/pubsub"
)

// defaultOpener create a default opener.
type defaultOpener struct {
	init   sync.Once
	opener *URLOpener
	err    error
}

func (o *defaultOpener) defaultOpener() (*URLOpener, error) {
	o.init.Do(func() {
		broker := os.Getenv("REDIS_URL")
		if broker == "" {
			o.err = errors.New("REDIS_URL environment variable not set")
			return
		}
		opt, err := redis.ParseURL(broker)
		if err != nil {
			o.err = err
			return
		}
		rdb := redis.NewClient(opt)
		o.opener = &URLOpener{
			Broker: rdb,
		}
	})
	return o.opener, o.err
}

func (o *defaultOpener) OpenTopicURL(ctx context.Context, u *url.URL) (*pubsub.Topic, error) {
	opener, err := o.defaultOpener()
	if err != nil {
		return nil, fmt.Errorf("open topic %v: %v", u, err)
	}
	return opener.OpenTopicURL(ctx, u)
}

func (o *defaultOpener) OpenSubscriptionURL(ctx context.Context, u *url.URL) (*pubsub.Subscription, error) {
	opener, err := o.defaultOpener()
	if err != nil {
		return nil, fmt.Errorf("open subscription %v: %v", u, err)
	}
	return opener.OpenSubscriptionURL(ctx, u)
}
