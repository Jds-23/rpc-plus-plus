#!/usr/bin/env bash
set -euo pipefail

BASE_URL=${BASE_URL:-http://0.0.0.0:8080}
REQUESTS=${REQUESTS:-3000}
CONCURRENCY=${CONCURRENCY:-10}
METHOD=${METHOD:-eth_blockNumber}

usage() {
    cat <<EOF
usage: scripts/load.sh [-n requests] [-c concurrency] [-u base_url] [-m method]

Posts JSON-RPC calls to \$base_url/rpc, then scrapes \$base_url/metrics.
Start the proxy first: cargo run

needs: oha, jq   (brew install oha jq, or cargo install oha)

defaults: -n $REQUESTS -c $CONCURRENCY -u $BASE_URL -m $METHOD
EOF
}

while getopts ":n:c:u:m:h" opt; do
    case $opt in
    n) REQUESTS=$OPTARG ;;
    c) CONCURRENCY=$OPTARG ;;
    u) BASE_URL=$OPTARG ;;
    m) METHOD=$OPTARG ;;
    h)
        usage
        exit 0
        ;;
    *)
        usage >&2
        exit 2
        ;;
    esac
done

for tool in oha jq; do
    if ! command -v "$tool" >/dev/null 2>&1; then
        echo "$tool not found — brew install $tool" >&2
        exit 1
    fi
done

if ! curl -sf -o /dev/null "$BASE_URL/healthz"; then
    echo "no proxy at $BASE_URL — start it with: cargo run" >&2
    exit 1
fi

RESULTS=$(mktemp)
trap 'rm -f "$RESULTS"' EXIT

echo "$REQUESTS requests, $CONCURRENCY at a time, $METHOD -> $BASE_URL/rpc"

oha --no-tui --output-format json -o "$RESULTS" \
    -n "$REQUESTS" -c "$CONCURRENCY" \
    -m POST \
    -H 'content-type: application/json' \
    -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"$METHOD\",\"params\":[]}" \
    "$BASE_URL/rpc"

echo
echo "--- client ---"
jq -r '
    "requests   \([.statusCodeDistribution[]] | add) in \(.summary.total * 1000 | round / 1000)s (\(.summary.requestsPerSec | round) req/s)",
    "latency    p50 \(.metrics.latency_ms.p50)ms  p95 \(.metrics.latency_ms.p95)ms  p99 \(.metrics.latency_ms.p99)ms  max \(.metrics.latency_ms.max)ms  mean \(.metrics.latency_ms.mean)ms",
    (.statusCodeDistribution | to_entries[] | "http \(.key)   \(.value)"),
    (.errorDistribution | to_entries[] | "error \(.key)   \(.value)")
' "$RESULTS"

echo
echo "--- /metrics ---"
curl -s "$BASE_URL/metrics" | grep -E 'rpc_attempts_total\{|_seconds_(count|sum)\{'
