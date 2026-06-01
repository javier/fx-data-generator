#!/usr/bin/env bash
# Backfill a window of FX data, faster-than-life (flat-out, no wall-clock wait).
# Bounded by --end_ts; --total_market_data_events is a safety cap. The start is
# advanced past any existing rows, so it continues strictly after the last row.
#
# Set your HA fleet + token file first (kept OUT of the repo):
#   export HOSTS="h1:9000,h2:9000,h3:9000"
#   export TOKEN_FILE="$HOME/qwp_token.txt"
# Optionally override the window / suffix:
#   START=2026-06-01T00:00:00.000000Z END=2026-06-02T00:00:00.000000Z ./run_backfill.sh
set -euo pipefail
cd "$(dirname "$0")"

HOSTS="${HOSTS:-h1:9000,h2:9000,h3:9000}"
TOKEN_FILE="${TOKEN_FILE:-$HOME/qwp_token.txt}"
START="${START:-2026-06-01T00:00:00.000000Z}"
END="${END:-2026-06-02T00:00:00.000000Z}"
SUFFIX="${SUFFIX:-_xxx}"

mvn -q compile
exec mvn -q exec:java -Dexec.args="--mode faster-than-life \
--hosts ${HOSTS} \
--tls_insecure --token_file ${TOKEN_FILE} \
--trades_processes 1 --market_data_processes 1 --core_processes 1 \
--market_data_min_eps 8200 --market_data_max_eps 11000 \
--core_min_eps 700 --core_max_eps 1000 \
--orders_min_per_sec 25 --orders_max_per_sec 140 \
--min_levels 1 --max_levels 2 \
--start_ts ${START} --end_ts ${END} \
--total_market_data_events 2850000000 \
--short_ttl true --enterprise true --lei_pool_size 2000 --suffix ${SUFFIX}"
