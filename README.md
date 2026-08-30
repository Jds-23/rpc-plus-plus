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
| `GET /metrics` | Prometheus exposition: attempts by outcome, attempt duration |

Deliberately not here yet: caching, least-latency routing, hedging, in-flight dedup, auth, multichain. Known gaps are tracked in [docs/design/v0.1.md](docs/design/v0.1.md#known-bugs) — read it before deploying this anywhere real.

## Run

```sh
cp settings.example.yaml settings.yaml   # then fill in your upstreams
export ALCHEMY_KEY=...                   # or write it to a gitignored .env
cargo run
```

`settings.yaml` is gitignored — a URL written out in full embeds the API key in its path.

```yaml
application:
  port: 8080                  # required
  host: 127.0.0.1             # default 127.0.0.1
  proxy:
    max_attempt: 3            # default 3
    retry_after_in_secs: 1    # default 1
    rpc_timeout_in_secs: 3    # default 3

upstreams:                    # required, at least one
  - label: alchemy
    url: https://eth-mainnet.g.alchemy.com/v2/${ALCHEMY_KEY}
  - label: drpc
    url: https://lb.drpc.org/ogrpc?network=ethereum&dkey=${DRPC_KEY}

decider: ROUND_ROBIN          # default ROUND_ROBIN, or PREFER_LEAST_ERRORS
```

Only `application.port` and `upstreams` are required. Upstreams are referred to by `label` everywhere in the logs; the URL is never logged.

`decider` picks the selection strategy. `ROUND_ROBIN` hands out every upstream in turn. `PREFER_LEAST_ERRORS` ranks them by error rate over a rolling window and re-ranks on a 15s timer, logging a `ranking_rebuilt` line each time. An unrecognised value is rejected while the config is read.

The pre-rename spellings still load. `rpcs:` and `rpc_url:` are accepted silently as aliases for `upstreams:` and `url:`; a top-level `rpc_timeout_in_secs` still wins over the one under `application.proxy` and logs one `config_key_deprecated` warn at startup.

`${VAR}` is expanded from the environment at startup, so keys stay out of the file. An unset or empty variable stops startup with a `startup_failed` line naming the **variable** — never the URL. A bare `$VAR` is literal text; the braces are required. Labels must be unique, and no two upstreams may share a URL: both are startup failures, since a duplicate label merges two providers into one identity and a duplicate URL turns failover into the same provider twice.

Set `RPC_CONFIG_PATH` to read the config from somewhere other than `./settings.yaml`.

`application.host` stays on loopback unless you widen it deliberately — inside a container it has to be `0.0.0.0`. There is no auth yet, so a reachable proxy is an open relay spending your upstream API keys.

## Layout

```
src/
  main.rs        init, load, build, run, and the signal watcher
  app.rs         composition root — Application, build_decider, build
  config.rs      settings.yaml -> Settings: env expansion, validation
  telemetry.rs   tracing subscriber
  http/          the axum surface — build_router plus one handler per route
  proxy/         Pipeline: the retry loop; attempt.rs one call, jsonrpc.rs the wire format
  upstream/      Upstream, UpstreamId, build_all; call.rs holds the per-call vocabulary
  decider/       the Decider trait, round_robin.rs, prefer_least_errors/
  observer/      the Observer seam and MetricsObserver; snapshot.rs values, prometheus.rs exposition
```

### Renamed in the v0.2.1 re-layer

| Old | New |
|---|---|
| `settings.rs`, `RpcSettings { label, rpc_url }` | `config.rs`, `UpstreamSettings { label, url }` |
| yaml `rpcs:` / `rpc_url:` / top-level `rpc_timeout_in_secs` | `upstreams:` / `url:` / `application.proxy.rpc_timeout_in_secs` |
| `start_up.rs`, `build_upstreams` | `app.rs`, `upstream::build_all` |
| `route/`, `rpc_proxy` / `get_health` | `http/`, `post_rpc` / `get_healthz` |
| `ProxyState`, `check_if_batch` | `proxy::Pipeline`, `proxy::jsonrpc::is_batch` |
| `StatsSnapshot`, `DiffStats` | `observer::snapshot::Snapshot`, `observer::snapshot::Diff` |
| `MetricsCollector` (in `route/metrics.rs`) | `observer::prometheus::Collector` |
| `Cached`, `PreferLeastErrorBuildError` and the other three `*BuildError` | `Ranking`, one `BuildError` per module |
| `UpstreamBuilder`, `ProxyStateBuilder`, `PreferLeastErrors::spawn(..6 args)` | `bon` builders on `Upstream`, `Pipeline`, `PreferLeastErrors`, `Application` |

Old config files still load — see the deprecation note above. `NoopObserver` and `CallError::as_str` were unused and are gone.

## Docs

- [docs/spec.md](docs/spec.md) — product requirements, full feature set
- [docs/design/v0.1.md](docs/design/v0.1.md) — v0.1 design, log schema, known bugs
- [docs/design/v0.2.1-DECIDER.md](docs/design/v0.2.1-DECIDER.md) — the windowed error-rate decider
- [docs/roadmap.md](docs/roadmap.md) — what lands next
- [docs/commit-style.md](docs/commit-style.md) — commit convention
