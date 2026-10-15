# The card

Twenty lines, no more. If a rule does not fit, it was not the 20%. Fill one
block per exercise, in the exercise's own words — not the docs'.

## The window (Ex1, Ex7)
Score the window (current − oldest baseline), never lifetime. Empty window = `None`, not zero.
Time-decayed EWMA forgets by time, but only when a sample lands; a window needs no per-call state.

## Which number (Ex2, Ex3)
Mean hides the tail; p50 hides it more. Pick the quantile on purpose.
A failure costs `TIMEOUT`, not its own 2ms — `src/` needs `failed_micros_total` for that.

## What the stats never saw (Ex4)
Record-after-await loses every dropped leg. RAII guard: armed on start, disarmed on finish, `Drop` records `Abandoned`.

## Measuring who you don't call (Ex5)
A slow head never spills traffic. 1-in-N deterministic, rotating probes keep challengers measured.

## Holding still (Ex6 — one paragraph on the herd)
Margin (head × 0.8) kills noise flaps. It cannot stop a herd: the head is slow because it is the head, and idle beats loaded every tick. The fix is comparing loaded to loaded, or not sending the head 100% — not in `rank`.

## The loop (Ex8)
snapshot → diff oldest → score with `MIN_SAMPLES` → stable sort, head × margin → one `store` → push, trim.
Detection lags an extra tick: slow calls record only when they finish.
