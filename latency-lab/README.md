# latency-lab

Eight drills for the least-latency decider — gap #8 in
`docs/erpc-gap-priorities.md`, spec §3.1 and §4.1 — done before it is written.
The `tokio-lab` / `dedup-lab` of this branch. Plan:
`~/.claude/plans/similar-to-chore-tokio-lab-and-curried-swan.md`.

Standalone crate — the root `Cargo.toml` has no `[workspace]` table, so nothing
here reaches the proxy's build, lockfile or CI.

## The shape of every file

**GIVEN:** the fakes, the counters, the clock, and every assert. The assert
messages say what a failure *means*, so read them — they are the teaching.
Tests marked "GIVEN, the trap" pass before you write anything: they show the
failure the blank exists to fix.

**BLANK:** one function, signature and contract given, body `todo!()`. That
function is the primitive.

```
cargo test --bin ex3_fast_failure   # red: panics at the todo!(). Read the asserts.
#   ... fill the blank ...
cargo test --bin ex3_fast_failure   # green: that is the grade
cargo run  --bin ex3_fast_failure   # the demo — compare against your prediction
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
| `ex1_window_mean` | 20 | `window_mean` | a slow *now* hidden by a fast lifetime |
| `ex2_quantile_from_buckets` | 40 | `quantile` | a mean that hides the tail |
| `ex3_fast_failure` | 40 | `score` | the 500-in-2ms upstream winning the head |
| `ex4_dropped_leg` | 45 | `guarded_attempt` | hedge losers never recorded — survivorship |
| `ex5_starved_challenger` | 40 | `route` | a slow head nobody can measure past |
| `ex6_flap_and_herd` | 35 | `rank` | flapping on noise; the herd a margin can't stop |
| `ex7_ewma_vs_window` | 30 | `TimeDecayed::observe` | an idle upstream remembered forever |
| `ex8_refresher` | 45 | `Refresher::refresh` | spec §4.1, end to end |

**Never cut ex3 or ex4** — one owns the wrong-winner failure, the other the
lie in the data every later drill reads. Cut list if the day slips: ex7 →
ex6's herd test → ex2's interpolation (nearest bucket bound is fine).

## What the drills say about `src/`

Read these after the drills, not before:

- `Snapshot` (`src/observer/snapshot.rs`) has one `duration_micros_total` for
  every outcome. Ex3's score needs failures' time split out.
- `try_once` (`src/proxy/attempt.rs`) records after the `.await`. Ex4 is what
  that costs once the hedger drops legs.
- `walk` sends #2 traffic only on failure. Ex5 is why a latency decider needs
  probes that an error-rate decider got for free.

## Stuck?

`cat solutions/exN_*.rs` — worked versions of all eight, gitignored and outside
`src/`, so cargo never compiles them. Read it, close it, **retype it from
memory**. That last step is the point; copy-paste teaches nothing.

`src/lib.rs` is scaffolding, not lesson: `Stats` / `Snapshot` stand in for
`UpstreamStats` / `Snapshot`, same bucket bounds, plus `failed`, `abandoned`
and `failed_micros_total`. `Fake` stands in for `Upstream::call`; its latency
and verdict can change mid-test, and `Fake::attempt` is `try_once` — call,
*then* record.
