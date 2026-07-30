# Product Requirements Document — Ethereum JSON-RPC Proxy (Rust)

## 1. Problem Statement

Infra teams running Ethereum applications depend on third-party RPC providers — plain RPC URLs pointing to services like Alchemy, Infura, QuickNode, or self-hosted nodes. These providers can go slow, return errors, or go dark entirely. When that happens, the outage reaches end users directly. Historical data is re-fetched repeatedly, wasting money and latency. And there is no single place to understand which upstream is slow and why.

This project is a production-deployable Rust service that sits between applications and N Ethereum JSON-RPC upstream URLs, providing request deduplication, finalized-data caching, least-latency routing, and a failsafe delivery chain — all with full Prometheus + Grafana observability.

**Primary user:** Infra teams self-hosting their own RPC infrastructure.

---

## 2. Goals & Success Criteria

| Goal | Success Criteria |
|---|---|
| Provider outages do not reach users | A failing upstream is removed from routing transparently; traffic shifts to healthy upstreams |
| Least-latency upstream is always preferred | Requests are routed to whichever upstream is currently fastest, based on observed latency |
| Finalized data is never re-fetched | Redis cache serves repeated requests for finalized blocks without hitting any upstream |
| In-flight duplicate requests are coalesced | Concurrent identical requests result in a single upstream call |
| Infra teams can observe the full fleet | A Grafana dashboard backed by Prometheus shows upstream health, latency, cache hit rate, and error rates |
| Production deployable | A Docker image + `docker-compose.yml` wires up the proxy, Redis, and Grafana out of the box |

---

## 3. Core Features (MVP, in priority order)

### 3.1 Failsafe Chain
The proxy manages N configured upstream RPC URLs. The failsafe chain operates as follows:
- **Least-latency routing:** Requests are routed to the upstream with the lowest observed latency. Latency scores are continuously updated based on real request performance.
- **Failover:** A failing upstream is immediately removed from the routing pool. Traffic shifts to the next best upstream automatically.
- **Hedging:** If the selected upstream exceeds a configurable latency threshold, a hedge request is silently raced against the next-best upstream. The first successful response wins.
- Hedge latency threshold is **configurable per upstream**.

### 3.2 Prometheus + Grafana Observability
- A single `/metrics` Prometheus endpoint covers the full fleet.
- Metrics include: request rate, per-upstream latency (p50/p95/p99), cache hit/miss rate, failover events, hedge events, and per-upstream error rate.
- A pre-built Grafana dashboard (provisioned via `docker-compose.yml`) visualizes all of the above.

### 3.3 Redis-Backed Cache
- Responses for finalized Ethereum data (e.g., requests scoped to finalized blocks) are cached in Redis.
- Cache entries persist across proxy restarts.
- Cache hits bypass all upstream calls entirely.

### 3.4 In-Flight Deduplication
- Concurrent identical JSON-RPC requests (same method + same params) are coalesced into a single upstream call.
- All waiting callers receive the same response when the single in-flight request completes.

### 3.5 Auth Middleware (deprioritized)
- Inbound requests are validated against a static list of API keys.
- Lowest priority for MVP; can be added after core features are stable.

---

## 4. User Stories / Use Cases

### 4.1 Slow upstream triggers least-latency rerouting
An infra team has three upstream RPC URLs configured. One starts degrading and responding slowly. The proxy's latency tracking detects this and begins routing new requests to the two faster upstreams. The Grafana dashboard shows the latency divergence and the routing shift, giving the team clear signal to investigate the slow provider.

### 4.2 Upstream goes dark
One upstream URL stops responding entirely. The proxy detects the failure and removes it from the routing pool immediately. Remaining upstreams absorb the traffic with no impact to the application. When the upstream recovers, it is re-admitted to the pool.

### 4.3 Slow upstream triggers hedging
The currently selected upstream exceeds its configured hedge threshold mid-request. The proxy silently fires a parallel request to the next-best upstream. The faster response wins and is returned to the caller. The Grafana dashboard shows a spike in hedge events.

### 4.4 Historical data requested repeatedly
Multiple application instances request logs for a finalized block. The first request is a cache miss — the proxy fetches from the upstream and writes the result to Redis. All subsequent requests for the same data are served from Redis with no upstream calls, reducing cost and latency.

---

## 5. Scope & Constraints

### In Scope
- Ethereum JSON-RPC proxying (HTTP)
- N upstream RPC URLs in the failsafe chain
- Least-latency routing across all configured upstreams
- In-flight request deduplication
- Redis-backed cache for finalized data
- Failover (immediate, on single failure) with automatic re-admission on recovery
- Per-upstream configurable hedge latency threshold
- Prometheus `/metrics` endpoint
- Pre-built Grafana dashboard
- YAML + environment variable configuration
- RPC URLs stored in YAML; API keys injected via environment variables (never stored in YAML)
- Docker image + `docker-compose.yml` deployment (proxy + Redis + Grafana)

### Out of Scope (MVP)
- WebSocket / subscription support
- Dynamic API key management or per-key rate limiting
- Kubernetes manifests
- Admin UI or control plane
- Upstream auto-discovery

### External Dependencies
- Redis (cache store)
- Prometheus (metrics scraping)
- Grafana (dashboard rendering)
- N Ethereum JSON-RPC upstream URLs (e.g., Alchemy, Infura, QuickNode, self-hosted Reth/Geth)

### Constraints
- 4-week build timeline
- Delivered as a single deployable binary + Docker image
- API keys must never appear in YAML config — injected via environment variables only