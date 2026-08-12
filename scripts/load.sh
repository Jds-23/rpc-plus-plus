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

if ! curl -sf -o /dev/null "$BASE_URL/healthz"; then
    echo "no proxy at $BASE_URL — start it with: cargo run" >&2
    exit 1
fi

RESULTS=$(mktemp)
trap 'rm -f "$RESULTS"' EXIT

echo "$REQUESTS requests, $CONCURRENCY at a time, $METHOD -> $BASE_URL/rpc"

started=$(date +%s)

seq 1 "$REQUESTS" | xargs -P "$CONCURRENCY" -I{} \
    curl -s -o /dev/null -w '%{http_code} %{time_total}\n' \
    -X POST "$BASE_URL/rpc" \
    -H 'content-type: application/json' \
    -d "{\"jsonrpc\":\"2.0\",\"id\":{},\"method\":\"$METHOD\",\"params\":[]}" \
    >"$RESULTS"

elapsed=$(($(date +%s) - started))

echo
echo "--- client ---"
awk '{ print $2 }' "$RESULTS" | sort -n | awk -v elapsed="$elapsed" '
    function percentile(fraction,   index_) {
        index_ = int(NR * fraction) + 1
        return time[index_ > NR ? NR : index_]
    }
    { time[NR] = $1; total += $1 }
    END {
        printf "requests   %d in %ds (%.1f req/s)\n", NR, elapsed, NR / (elapsed > 0 ? elapsed : 1)
        printf "latency    p50 %.3fs  p95 %.3fs  max %.3fs  mean %.3fs\n",
            percentile(0.50), percentile(0.95), time[NR], total / NR
    }
'
awk '{ print $1 }' "$RESULTS" | sort | uniq -c | awk '{ printf "http %s   %d\n", $2, $1 }'

echo
echo "--- /metrics ---"
curl -s "$BASE_URL/metrics" | grep -E 'rpc_attempts_total\{|_seconds_(count|sum)\{'
