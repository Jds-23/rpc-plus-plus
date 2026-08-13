# rpc-plus-plus

An Ethereum JSON-RPC proxy that sits between your app and N upstream providers.

## Why

Apps hardcode a single RPC URL. When that provider rate-limits, slows down, or goes dark, the outage reaches users directly — and the fix is a manual endpoint swap and a redeploy. Every service ends up managing its own list of URLs, and when a request fails there is no single place that says which upstream failed or why.

## Goals

- **Provider outages do not reach users.** If at least one upstream is healthy, the caller should not see the failure.
- **One request is debuggable from logs alone.** Follow a single `request_id` across every upstream attempt and see which provider failed, with what status, and how long it took.

## Status — v0.1

Round-robin across upstreams with serial retry failover. Logs are the only observability surface.

| | |
|---|---|
| `POST /rpc` | Raw JSON-RPC passthrough. Batches rejected with HTTP 400 |
| `GET /healthz` | Liveness |

Deliberately not here yet: caching, least-latency routing, hedging, in-flight dedup, metrics, auth, multichain. Known gaps are tracked in [docs/design/v0.1.md](docs/design/v0.1.md#known-bugs) — read it before deploying this anywhere real.

## Run

```sh
cp settings.example.yaml settings.yaml   # then fill in your upstreams
export ALCHEMY_KEY=...                   # or write it to a gitignored .env
cargo run
```

`settings.yaml` is gitignored — a URL written out in full embeds the API key in its path.

```yaml
application_port: 8080
rpcs:
  - label: alchemy
    rpc_url: https://eth-mainnet.g.alchemy.com/v2/${ALCHEMY_KEY}
  - label: drpc
    rpc_url: https://lb.drpc.org/ogrpc?network=ethereum&dkey=${DRPC_KEY}
application_host: 127.0.0.1  # default 127.0.0.1
max_attempt: 3            # default 3
rpc_timeout_in_secs: 3    # default 3
retry_after_in_secs: 1    # default 1
```

Only `application_port` and `rpcs` are required. Upstreams are referred to by `label` everywhere in the logs; the URL is never logged.

`${VAR}` is expanded from the environment at startup, so keys stay out of the file. An unset or empty variable stops startup with a `startup_failed` line naming the **variable** — never the URL. A bare `$VAR` is literal text; the braces are required. Labels must be unique, and no two upstreams may share a URL: both are startup failures, since a duplicate label merges two providers into one identity and a duplicate URL turns failover into the same provider twice.

Set `RPC_CONFIG_PATH` to read the config from somewhere other than `./settings.yaml`.

`application_host` stays on loopback unless you widen it deliberately — inside a container it has to be `0.0.0.0`. There is no auth yet, so a reachable proxy is an open relay spending your upstream API keys.

## Docs

- [docs/spec.md](docs/spec.md) — product requirements, full feature set
- [docs/design/v0.1.md](docs/design/v0.1.md) — v0.1 design, log schema, known bugs
- [docs/commit-style.md](docs/commit-style.md) — commit convention
