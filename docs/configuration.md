# Configuration reference

The proxy reads its settings from `./settings.yaml` at startup. To read them from another file, set `RPC_CONFIG_PATH`. `settings.yaml` is gitignored because a URL with a key in it is a secret.

## Full example with defaults

Only `application.port` and `upstreams` are required. Every other key below shows its default.

```yaml
application:
  port: 8080                  # required
  host: 127.0.0.1
  proxy:
    max_attempt: 3
    retry_after_in_secs: 1
    rpc_timeout_in_secs: 3
    hedge:
      enabled: false
      after_in_millis: 250
    dedup:
      enabled: false
  cors:
    allowed_origins: []

upstreams:                    # required, at least one
  - label: alchemy
    url: https://eth-mainnet.g.alchemy.com/v2/${ALCHEMY_KEY}
    hedge_after_in_millis: 400  # optional, overrides hedge.after_in_millis
  - label: drpc
    url: https://lb.drpc.org/ogrpc?network=ethereum&dkey=${DRPC_KEY}

decider: ROUND_ROBIN
```

## `application`

`port` is the port the proxy listens on.

`host` is the address the proxy binds to. The default is loopback. Inside a container, `host` must be `0.0.0.0`. The proxy has no authentication, so a proxy that other hosts can reach is an open relay that spends your upstream API keys.

## `application.proxy`

`max_attempt` is the largest number of upstream calls that one request can make.

`retry_after_in_secs` is the wait before a retry.

`rpc_timeout_in_secs` is how long the proxy waits for one upstream call. The `LEAST_LATENCY` decider also scores a failed call as this long.

### `hedge`

When `enabled` is `true`, the proxy sends a read to the next upstream in the chain after the current upstream has gone `after_in_millis` without an answer. The proxy returns the first answer. The proxy never hedges writes. With hedging on, a write goes to the first upstream in the chain only, and the proxy does not retry it on another upstream.

`after_in_millis` must be at least 1. `upstreams[].hedge_after_in_millis` overrides it for one upstream and must also be at least 1. Design: [v0.3-HEDGING.md](design/v0.3-HEDGING.md).

### `dedup`

When `enabled` is `true`, concurrent requests with the same `method` and `params` share one upstream call. Each caller gets a response with its own `id`. The proxy coalesces only idempotent reads. Writes, notifications, and unknown methods always make their own call.

Dedup is not a cache. A request that arrives after the shared call has finished makes a new call. Hedging and dedup work together. Each coalesced request logs a `request_coalesced` line with the `leader_request_id` of the request whose upstream attempts it shared. Design: [v0.4-DEDUP.md](design/v0.4-DEDUP.md).

## `application.cors`

`allowed_origins` lists the origins whose pages can call `/rpc` from a browser.

- An empty list sends no CORS headers.
- `["*"]` allows any origin. `*` must be the only entry.
- Write each origin exactly as a browser sends it in `Origin`: lowercase `scheme://host[:port]`, with no path, no trailing slash, and no default port. Any other spelling can never match, so it stops startup.

The proxy allows only `POST` with a `Content-Type` header. Browsers can cache the preflight response for one hour. CORS controls only which pages can read the responses. CORS is not access control, because curl and backends ignore it.

## `upstreams`

Each upstream has a `label` and a `url`. Logs and metrics name an upstream by its `label`. The proxy never logs the URL.

The proxy rejects these lists at startup:

- An empty list.
- An empty `label` or an empty `url`.
- Two upstreams with the same `label`. A duplicate label would merge two providers into one identity.
- Two upstreams with the same `url`. A duplicate URL would make failover call the same provider twice.

### Environment variables in URLs

The proxy expands `${VAR}` from the environment at startup, so keys stay out of the file. An unset or empty variable stops startup with a `startup_failed` line that names the variable and not the URL. A bare `$VAR` is literal text. The braces are required.

## `decider`

`decider` picks the order in which the proxy tries the upstreams. An unknown value stops startup.

| Value | Order |
|---|---|
| `ROUND_ROBIN` | Each upstream in turn. |
| `PREFER_LEAST_ERRORS` | Lowest error rate first, over a rolling window. The ranking rebuilds every 15 seconds and logs a `ranking_rebuilt` line. Design: [v0.2.1-DECIDER.md](design/v0.2.1-DECIDER.md). |
| `LEAST_LATENCY` | Lowest mean response time first, over a rolling window. A failed call counts as `rpc_timeout_in_secs`. The ranking rebuilds every 15 seconds. One request in 20 goes to a lower-ranked upstream first, so the proxy keeps a current time for every upstream. Design: [v0.5-LATENCY.md](design/v0.5-LATENCY.md). |

## Old key names

Config files from before v0.2.1 still load.

- `rpcs:` is an alias for `upstreams:`.
- `rpc_url:` is an alias for `url:`.
- A top-level `rpc_timeout_in_secs` overrides `application.proxy.rpc_timeout_in_secs` and logs one `config_key_deprecated` warning at startup.
