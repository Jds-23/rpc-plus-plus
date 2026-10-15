# eRPC gaps, ranked by value for effort

Every ✗ from [erpc-comparison.md](erpc-comparison.md), ordered so the cheapest high-value work
comes first. Effort is an estimate against today's code: **S** fits in one PR, **M** needs a
design doc, **L** spans releases. Value is weighed against [spec.md](spec.md) and the risks
in the README.

## Tier 1 — quick wins (S effort, high value)

| # | Gap | Effort | Value | Why it pays | Where it lands |
|---|---|---|---|---|---|
| 1 | Static API-key auth | S | High | A reachable proxy is an open relay spending upstream keys (README). Spec §3.5 asks for exactly a static key list. | axum middleware in `http/`; key list in `config.rs` via `${VAR}` |
| 2 | Upstream ejection (circuit breaker) | S | High | Spec §4.2: a dark upstream leaves the pool *immediately* and comes back on recovery. `PREFER_LEAST_ERRORS` only re-ranks it every 15s, it never drops it. | Threshold + cooldown on the ranking in `decider/prefer_least_errors/` |
| 3 | Readiness in `/healthz` | S | Medium | Today it returns `OK` unconditionally, so a proxy with every upstream down still looks healthy to Docker / a load balancer. | `http/healthz.rs`: report not-ready when `decide(1)` is empty |
| 4 | Grafana dashboard | S | Medium | Spec §3.2 deliverable; the metrics already exist (attempts, duration, hedges, hedge wins). | Provisioned JSON in `docker-compose.yml` |
| 5 | CORS | S | Medium | Browser dapps can't call the proxy at all without it. | `tower_http::cors::CorsLayer` in `http/mod.rs`, origins in config |
| 6 | Hedge `eth_sendRawTransaction` | S | Medium | Faster mempool propagation. Safe because a signed raw tx is idempotent by hash and nonce. | Let it through `is_write` in the hedger only; map a slower leg's "already known" to success |

## Tier 2 — bigger bets worth making (M–L effort, high value)

| # | Gap | Effort | Value | Why it pays | Where it lands |
|---|---|---|---|---|---|
| 7 | JSON-RPC batch support | M | High | viem and ethers batch by default; today those clients get HTTP 400. | Fan each element through `Pipeline`, reassemble by `id`; reuses dedup + hedge per element |
| 8 | Least-latency decider | M | High | Spec §3.1, first feature listed. Attempt duration is already observed. | New `Decider` beside `prefer_least_errors/`, same refresher pattern |
| 9 | Per-upstream rate limits | M | Medium | Keeps hedging and retries from blowing provider quotas; eRPC warns hedges are paid duplicates. | Token bucket per `Upstream`; a limited upstream is skipped like a failing one |
| 10 | Redis finality-aware cache | L | High | Spec §3.3 / §4.4: repeated finalized reads cost nothing. Next release per the v0.4 doc. | New layer ahead of singleflight; needs finality rules per method |

## Tier 3 — nice to have (M effort, low–medium value)

| # | Gap | Effort | Value | Note |
|---|---|---|---|---|
| 11 | `eth_getLogs` range splitting | M | Medium | Helps providers with block-range caps; depends on batch-style fan-out (#7). |
| 12 | Error normalization | M | Low–Medium | Per-provider error mapping; ongoing maintenance. |
| 13 | OpenTelemetry tracing | M | Low | `request_id` across attempts already makes one request debuggable from logs alone. |
| 14 | `validate` / `dump` CLI | S | Low | Startup already validates config and fails loudly. |

## Tier 4 — skip for now (L effort, outside scope)

| Gap | Why skip |
|---|---|
| Multichain, multi-tenant projects | Reshapes routing, config and metrics; spec targets a single Ethereum chain. |
| Consensus / integrity checks | Multiplies upstream cost per request; needs a response comparison model. |
| Provider auto-config | Spec §5 lists upstream auto-discovery as out of scope. |
| Admin API, shadow upstreams, static responses | Spec §5 lists an admin control plane as out of scope. |
