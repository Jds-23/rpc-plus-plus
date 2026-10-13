# ratelimit-lab

Seven drills for per-upstream rate limits — gap #9 in
[`docs/erpc-gap-priorities.md`](../docs/erpc-gap-priorities.md): "token bucket
per `Upstream`; a limited upstream is skipped like a failing one", there to keep
hedges and retries from blowing provider quotas. Done before any of it is
written. Plan: `~/.claude/plans/branch-of-current-2-floofy-dongarra.md`.

Standalone crate — the root `Cargo.toml` has no `[workspace]` table, so nothing
here reaches the proxy's build, lockfile or CI.

## The shape of every file

**GIVEN:** the fakes, the clock, the broken designs, and every assert. The
assert messages say what a failure *means*, so read them — they are the
teaching. Each drill has at least one test marked `GIVEN, and this is the bug`
that already passes: the wrong answer, working.

**BLANK:** one function (a pair in ex5 and ex6), signature and contract given,
body `todo!()`. That function is the primitive.

```
cargo test --bin ex3_skip_limited   # red: panics at the todo!(). Read the asserts.
#   ... fill the blank ...
cargo test --bin ex3_skip_limited   # green: that is the grade
cargo run  --bin ex3_skip_limited   # the demo — compare against your prediction
```

Two rules:

1. The `PREDICTION` block is written **before** the code. Every time.
2. Buzzer beats perfection. At the budget (in each file's header), write the
   `RESULT` block and move on — a drill that ended in a surprise is worth more
   than one polished into agreement.

Then add a line to `CARD.md` and commit.

## Order and budgets

| Drill | Budget | Blank | Owns |
|---|---|---|---|
| `ex1_token_bucket` | 30 | `TokenBucket::try_acquire` | truncated refill starves a busy bucket |
| `ex2_shared_limiter` | 30 | `call_limited` | wrong critical section: overspend, or a semaphore of 1 |
| `ex3_skip_limited` | 45 | `walk` | limiting that costs latency, attempts, or phantom charges |
| `ex4_provider_pushback` | 40 | `Limiter::on_rate_limited` | a full local bucket while the provider says 429 |
| `ex5_hedge_budget` | 40 | `HedgeBudget::deposit`, `try_spend` | the hedge storm: a slow primary doubles the bill |
| `ex6_compute_units` | 25 | `cost`, `TokenBucket::try_acquire_n` | counting requests when the provider counts CUs |
| `ex7_all_limited` | 60 | `serve` | all limited → `-32603` for a 5ms wait, or a hung request |

Setup 20 + drills 270 + card 20 ≈ 5h10m.

**Never cut ex3 or ex5** — one owns where the limiter sits in `walk`, the other
the reason the gap exists at all. Cut list if the day slips: ex4's
`Retry-After` half → ex7's deadline-wait half (keep fail-fast) → ex6's
unknown-method default.

## Stuck?

`cat solutions/exN_*.rs` — worked versions of all seven, gitignored and outside
`src/`, so cargo never compiles them. Read it, close it, **retype it from
memory**. That last step is the point; copy-paste teaches nothing.

`src/lib.rs` is scaffolding, not lesson: `Fake` stands in for `Upstream::call`
(`src/upstream/mod.rs`) and takes the call's cost in CUs. `Quota` is the
provider's side — a fixed window that answers 429 when full, with or without
`Retry-After`. `entered`, `rejected`, `finished` and `cu_served` are what the
provider saw. Its header lists every place it departs from the real code.

One thing `Quota` teaches on its own: a bucket of `rate` per second and `burst`
can put `burst + rate × 1s` into one provider window. Sizing it to the
provider's number means `burst + rate ≤ limit`, not `rate ≤ limit` — ex6 and
ex7 size theirs that way.

## What the drills say about `src/`

Read after the drills, not before.

- **ex1, ex2** — `Upstream` (`src/upstream/mod.rs`) has no field that changes
  after `build`; it is shared as `Arc<Upstream>` by every request. The limiter
  is its first mutable state: `&self` methods, one `std::sync::Mutex` (or a CAS
  loop), never held across `call`'s `.await`.
- **ex3** — `Core::walk` (`src/proxy/mod.rs`): the check goes after
  `if tried.len() >= self.max_attempt { break; }` and *before*
  `if !tried.is_empty() { sleep(retry_after) }` — and a skip is never
  `tried.push`ed. `Failure` needs a variant for "all skipped"; `NoUpstream`
  means the chain was empty. `race` (`src/proxy/hedge.rs`) needs the same:
  `launch` must skip forward past a limited `chain[index]`.
- **ex4** — `Upstream::call_inner` (`src/upstream/call.rs`) maps a non-200 to
  `CallError::ErrorStatus { http_status }` after `res.bytes()` has consumed the
  response: `Retry-After` is gone. Read the header before the body. A `-32005`
  in a 200 body (`src/jsonrpc.rs:69`, retryable) has no header at all — it
  always gets the default cooldown.
- **ex5** — the timer arm of `race`'s `select!` (`src/proxy/hedge.rs`) is where
  `try_spend` goes: `max_attempt` caps the hedges of one request, nothing caps
  them across requests. A refused spend keeps waiting on the in-flight call;
  `record_hedge` must not fire for it.
- **ex6** — the method is already parsed up to twice per request, by `is_write` and
  `dedup_key` when dedup is on (`src/jsonrpc.rs`). A CU cost would be a third parse: parse once
  in `Pipeline::proxy_inner` and pass it down.
- **ex7** — `finish` (`src/proxy/mod.rs`) answers every `Failure`, including
  `NoUpstream`, with `JSONRPC_INTERNAL_ERROR` (`-32603`). All-limited needs its
  own arm answering `-32005`. The deadline does not exist yet: today a request
  is bounded only by `max_attempt × (rpc_timeout + retry_after)`.
