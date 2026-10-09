# rpc-plus-plus vs eRPC

Feature comparison against [eRPC](https://github.com/erpc/erpc). rpc-plus-plus as of v0.4 (dedup);
eRPC from its README and [docs.erpc.cloud](https://docs.erpc.cloud/), read 2026-10-08.

| Feature | rpc-plus-plus | eRPC |
|---|---|---|
| Language | Rust (tokio, axum) | Go |
| Chains | One EVM chain, one upstream set | 2,000+ EVM chains plus Solana; `/<project>/<arch>/<chainId>` |
| Multi-tenant (projects) | ✗ | ✓ each project has its own networks, upstreams, auth and budgets |
| Provider auto-config | ✗ URLs by hand, `${VAR}` expansion | ✓ 23 vendor integrations, 4,000+ public endpoints |
| Upstream selection | `ROUND_ROBIN`, `PREFER_LEAST_ERRORS` (rolling error rate, re-ranked every 15s) | Scored on latency, errors and block lag every 15s; selection policies, lanes, admin cordon |
| Method routing | ✗ | ✓ routes each method to upstreams that support it |
| Retry / failover | ✓ serial; `max_attempt`, `retry_after_in_secs` | ✓ rotates upstream; retries for missing data paced to block time |
| Timeout | ✓ per call, `rpc_timeout_in_secs` | ✓ three nested layers |
| Hedging | ✓ opt-in; global threshold, per-upstream override; never hedges writes | ✓ can hedge `eth_sendRawTransaction` on EVM |
| Circuit breaker | ✗ | ✓ |
| In-flight dedup | ✓ opt-in; reads only; each caller gets its own `id` | ✓ |
| Caching | ✗ Redis cache planned (spec §3.3) | ✓ re-org and finality aware; memory, Redis, Postgres, DynamoDB, gRPC |
| Consensus / integrity | ✗ | ✓ cross-upstream agreement, drops stale or malformed responses, penalizes disagreeing nodes |
| JSON-RPC batches | ✗ rejected with HTTP 400 | ✓ split and run in parallel, re-batched for upstreams that support it |
| `eth_getLogs` splitting | ✗ | ✓ |
| Rate limits / budgets | ✗ | ✓ four layers, compute-unit budgets, auto-tuning |
| Auth | ✗ deprioritized (spec §3.5) | ✓ token, JWT, SIWE, basic auth, IP allowlist |
| CORS | ✗ | ✓ |
| Error normalization | ✗ passed through | ✓ same codes across providers |
| Health endpoint | ✓ `GET /healthz`, liveness only | ✓ 8 probe strategies; ready, draining or broken |
| Metrics | ✓ Prometheus `/metrics`: attempts by outcome, attempt duration, hedges, hedge wins | ✓ ~141 metrics, Grafana dashboard (80+ panels), alert rules |
| Tracing / logs | ✓ structured logs; one `request_id` across every attempt | ✓ plus OpenTelemetry |
| Graceful shutdown | ✓ SIGINT / SIGTERM | ✓ draining state |
| Admin API | ✗ | ✓ JSON-RPC admin API |
| Shadow upstreams / static responses | ✗ | ✓ |
| Config | YAML, validated at startup; `RPC_CONFIG_PATH` | YAML, TS or JS; `erpc validate`, `erpc dump` |
| Deploy | Dockerfile, docker-compose with Prometheus | Docker image, npx, Railway, Kubernetes manifests |

eRPC's docs don't cover WebSocket subscriptions, so that row is left out.
