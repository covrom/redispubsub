# redispubsub
Redis driver for https://godoc.org/gocloud.dev/pubsub package.

A great alternative to using Kafka, with the ability to quickly switch to it. You can use this driver for MVP, local, small or medium projects. When your project grows you can simply switch to another driver from https://pkg.go.dev/gocloud.dev/pubsub#section-directories.

Using Redis Streams, this driver supports `at-least-once` delivery.

The driver uses these Redis commands:
- XADD
- XGROUP CREATE
- XREADGROUP (with pending and then new messages - only this library actually supports it)
- XACK
- XAUTOCLAIM

Many other queuing implementations with Redis Streams contain a big bug. They incorrectly support reconnecting a consumer to a topic if a message has been received but not acknowledged. They use ">" streaming strategy, which does not deliver unacknowledged messages more than once. And you miss messages when microservices are restarted.
This library does not have this disadvantage.

## Connection to Redis
The connection string must be defined in the `REDIS_URL` environment value.

## Warning about creating a topic consumer group for the first time
All consumers already have a group, even if there is only one consumer in the group.

Consumer groups receive the same messages from the topic, and consumers within the group receive these messages exclusively.

![Messages flow](flow.png)

This driver supports new consumers joining with a new group name after the publisher has sent multiple messages to a topic before the group was created. These consumers will receive all previous non-ACK-ed messages from the beginning of the topic.

## How to open topic and send message
```go
import (
    _ "github.com/covrom/redispubsub"
    "gocloud.dev/pubsub"
)

ctx := context.Background()
topic, err := pubsub.OpenTopic(ctx, "redis://topics/1")
if err != nil {
    return fmt.Errorf("could not open topic: %v", err)
}
defer topic.Shutdown(ctx)

m := &pubsub.Message{
    Body: []byte("Hello, World!\n"),
    // Metadata is optional and can be nil.
    Metadata: map[string]string{
        // These are examples of metadata.
        // There is nothing special about the key names.
        "language":   "en",
        "importance": "high",
    },
}

err = topic.Send(ctx, m)
if err != nil {
    return err
}
```

OpenTopic connection string host/path is required and must contain the topic name.

Connection string query parameters:
- `maxlen` is MAXLEN parameter for XADD (limit queue length), unlimited if not set.

## How to subscribe on topic
```go
import (
    _ "github.com/covrom/redispubsub"
    "gocloud.dev/pubsub"
)

subs, err := pubsub.OpenSubscription(ctx, "redis://group1?consumer=cons1&topic=topics/1")
if err != nil {
    return err
}
defer subs.Shutdown(ctx)

msg, err := subs.Receive(ctx)
if err != nil {
    // Errors from Receive indicate that Receive will no longer succeed.
    return fmt.Errorf("Receiving message: %v", err)
}
// Do work based on the message, for example:
fmt.Printf("Got message: %q\n", msg.Body)
// Messages must always be acknowledged with Ack.
msg.Ack()
```

OpenSubscription connection string host(path) is required and must contain the consumer group name.

Connection string query parameters:
- `topic` is topic name, required
- `consumer` is unique consumer name, required
- `from` is FROM option for creating a consumer group (if not exists) with XGROUP CREATE, default is '0'
- `autoclaim` is *min-idle-time* option for XAUTOCLAIM, 30 min by default
- `noack` is NOACK option for XREADGROUP, not used by default

See [basic_test.go](basic_test.go) for full usage example.

## Monitoring with Prometheus & Grafana
Use [redis-exporter](https://github.com/oliver006/redis_exporter) prometheus exporter with `check-streams` option.

See [streams.go](https://github.com/oliver006/redis_exporter/blob/master/exporter/streams.go) for details.