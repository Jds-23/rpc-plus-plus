default:
    @just --list

# everything CI runs
ci: fmt-check clippy test doc

fmt:
    cargo fmt --all

fmt-check:
    cargo fmt --all --check

clippy:
    cargo clippy --all-targets --all-features -- -D warnings

# just test [filter...], e.g. `just test observer`
test *args:
    cargo nextest run --all-targets --all-features {{args}}

# one tests/<name>.rs file, e.g. `just test-file observer`
test-file name *args:
    cargo nextest run --all-features --test {{name}} {{args}}

doc:
    cargo test --doc --all-features

# rewrites files in place
fix:
    cargo fmt --all
    cargo clippy --all-targets --all-features --fix --allow-dirty --allow-staged -- -D warnings

run:
    cargo run
