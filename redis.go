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
	"gocloud.dev/pubsub"
)

func init() {
	opener := new(defaultOpener)
	pubsub.DefaultURLMux().RegisterTopic(Scheme, opener)
	pubsub.DefaultURLMux().RegisterSubscription(Scheme, opener)
}

// Scheme is the URL scheme that redispubsub registers its URLOpeners under on pubsub.DefaultMux.
const Scheme = "redis"
