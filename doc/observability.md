# Observability

YedMQ exposes broker state through two node-local surfaces:

- MQTT `$SYS/broker/*` topics
- REST API `GET /metrics`

Both surfaces describe the node that publishes or serves them. They do not claim to be cluster-global totals.

## `$SYS` Topics

`$SYS/broker/*` topics are published by each broker node for that same node. A subscriber connected to node `1001` should treat `$SYS/broker/clients/connected` as node `1001`'s connected client count.

The numeric payloads are UTF-8 decimal strings, for example:

```text
300
```

They are not binary integers. This keeps `$SYS` payloads readable from normal MQTT clients and avoids truncating values above 255.

Current local broker topics include:

- `$SYS/broker/clients/connected`
- `$SYS/broker/bytes/sent`
- `$SYS/broker/bytes/received`
- `$SYS/broker/uptime`
- `$SYS/broker/packets/received`
- `$SYS/broker/packets/sent`
- `$SYS/broker/messages/received`
- `$SYS/broker/messages/sent`
- `$SYS/broker/messages/dropped`
- `$SYS/broker/subscriptions/count`

Do not interpret `$SYS/broker/*` as a cluster-wide total. A future `$SYS/cluster/*` namespace can publish a small cluster summary, but it should use a deterministic publisher so multiple nodes do not overwrite the same retained topic with different observations.

## OpenMetrics Endpoint

Each node exposes a Prometheus/OpenMetrics-compatible endpoint through the REST API:

```bash
curl -u admin:replace_me http://127.0.0.1:3456/metrics
```

The endpoint is protected by the same Basic Auth configuration as the rest of the management API. Configure at least one user under `[listener.api.auth].users` before scraping it.

Every metric sample includes:

- `cluster`: the configured cluster name
- `node_id`: the current node id

Example:

```text
yedmq_node_info{cluster="YedMQ",node_id="1001"} 1
yedmq_clients_connected{cluster="YedMQ",node_id="1001"} 42
yedmq_messages_received_total{cluster="YedMQ",node_id="1001"} 1200
```

Scrape every broker node separately and aggregate outside YedMQ. For example, Prometheus can compute cluster totals with queries such as:

```text
sum(yedmq_clients_connected)
sum by (node_id) (yedmq_messages_received_total)
```

This design keeps broker nodes independent and lets monitoring detect exactly which node failed to scrape.
