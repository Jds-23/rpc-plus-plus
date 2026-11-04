# Source layout

```
src/
	main.rs        init, load, build, run, and the signal watcher
	app.rs         composition root: Application, build_decider, build
	config.rs      settings.yaml to Settings: env expansion and validation
	jsonrpc.rs     the wire format: request shape, write detection, dedup key, and re-addressing
	telemetry.rs   tracing subscriber
	http/          the axum routes: build_router and one handler per route
	proxy/         Pipeline, the retry loop. attempt.rs is one call, hedge.rs the race,
	               reply.rs the shared answer, and singleflight/ the in-flight dedup map
	upstream/      Upstream, UpstreamId, and build_all. call.rs holds the per-call types
	decider/       the Decider trait, round_robin.rs, prefer_least_errors/, and least_latency/
	observer/      the Observer trait and MetricsObserver. snapshot.rs holds the values,
	               prometheus.rs the exposition
```

## Names changed in v0.2.1

| Old | New |
|---|---|
| `settings.rs`, `RpcSettings { label, rpc_url }` | `config.rs`, `UpstreamSettings { label, url }` |
| yaml `rpcs:`, `rpc_url:`, and top-level `rpc_timeout_in_secs` | `upstreams:`, `url:`, and `application.proxy.rpc_timeout_in_secs` |
| `start_up.rs`, `build_upstreams` | `app.rs`, `upstream::build_all` |
| `route/`, `rpc_proxy`, and `get_health` | `http/`, `post_rpc`, and `get_healthz` |
| `ProxyState`, `check_if_batch` | `proxy::Pipeline`, `proxy::jsonrpc::is_batch` |
| `StatsSnapshot`, `DiffStats` | `observer::snapshot::Snapshot`, `observer::snapshot::Diff` |
| `MetricsCollector` in `route/metrics.rs` | `observer::prometheus::Collector` |
| `Cached`, `PreferLeastErrorBuildError`, and the other three `*BuildError` types | `Ranking`, and one `BuildError` per module |
| `UpstreamBuilder`, `ProxyStateBuilder`, and `PreferLeastErrors::spawn` with six arguments | `bon` builders on `Upstream`, `Pipeline`, `PreferLeastErrors`, and `Application` |

Old config files still load. See [old key names](configuration.md#old-key-names). `NoopObserver` and `CallError::as_str` had no callers and were deleted.
