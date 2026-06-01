#!/usr/bin/env bash
# Real-time HIGH-RATE demo: ~1M rows/sec (750k market_data + 240k core_price +
# ~25k trades) in real seconds. Runs until Ctrl+C. Rows are stamped a couple of
# seconds ahead of wall-clock (--realtime_lookahead_secs) so the live dashboard
# stays ahead of WAL apply lag.
#
# Set your HA fleet + token file first (kept OUT of the repo):
#   export HOSTS="h1:9000,h2:9000,h3:9000"
#   export TOKEN_FILE="$HOME/qwp_token.txt"
set -euo pipefail
cd "$(dirname "$0")"

HOSTS="${HOSTS:-h1:9000,h2:9000,h3:9000}"
TOKEN_FILE="${TOKEN_FILE:-$HOME/qwp_token.txt}"
SUFFIX="${SUFFIX:-_xxx}"

mvn -q compile
exec mvn -q exec:java -Dexec.args="--mode real-time \
--hosts ${HOSTS} \
--tls_insecure --token_file ${TOKEN_FILE} \
--trades_processes 1 --market_data_processes 3 --core_processes 1 \
--orders_min_per_sec 7150 --orders_max_per_sec 7150 \
--market_data_min_eps 750000 --market_data_max_eps 750000 \
--core_min_eps 240000 --core_max_eps 240000 \
--min_levels 5 --max_levels 5 \
--total_market_data_events 0 \
--short_ttl true --enterprise true --create_views true --suffix ${SUFFIX} \
--market_data_commit_interval_ms 750"
