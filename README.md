# redispubsub
Redis driver for https://godoc.org/gocloud.dev/pubsub package.
A great alternative to using Kafka, with the ability to quickly switch to it.

Using Redis Streams, this driver supports `at-least-once` delivery.

Used Redis commands:
- XADD
- XGROUP CREATE
- XREADGROUP (with pending and then new messages - only this library actually supports it)
- XACK

## Connection to Redis
The connection string must be defined in the `REDIS_URL` environment value.

## Warning about creating a topic consumer group for the first time
A consumer group (but not a consumer!) must be created before posting messages to topic with unattached consumers.
This driver does not support new consumers attaching with a new group name after the publisher has sent multiple messages to a topic, because they do not receive previous messages.

You can attach topic consumers before attach publishers, or create groups manually: 

```go
opt, err := redis.ParseURL(os.Getenv("REDIS_URL"))
if err != nil {
    return err
}
rdb := redis.NewClient(opt)

if _, err := rdb.XGroupCreateMkStream(context.Background(),
    // here $ is needed, see https://redis.io/commands/xgroup-create/
    "topics/1", "group1", "$").Result(); err != nil {
    return err
}
```

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

See [basic_test.go](basic_test.go) for full usage example.

## Monitoring with Prometheus & Grafana
Use [redis-exporter](https://github.com/oliver006/redis_exporter) prometheus exporter with `check-streams` option.

See [streams.go](https://github.com/oliver006/redis_exporter/blob/master/exporter/streams.go) for details.