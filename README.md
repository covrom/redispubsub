# redispubsub
Redis driver for https://godoc.org/gocloud.dev/pubsub package

Used Redis commands:
- XADD
- XGROUP CREATE
- XREADGROUP
- XACK

See [basic_test.go](basic_test.go) for usage example.

## Monitoring with Prometheus & Grafana
Use [redis-exporter](https://github.com/oliver006/redis_exporter) prometheus exporter with `check-streams` option.

See [streams.go](https://github.com/oliver006/redis_exporter/blob/master/exporter/streams.go) for details.