#!/usr/bin/env bash
# Real-time "regular" demo: a few thousand rows/sec in real seconds, matching the
# Python generator's everyday rates (~1,200-2,500 market_data/s + ~850 core/s +
# ~tens of trades/s). Same shape as the backfill but paced to wall-clock. Runs
# until Ctrl+C. Use this for normal demos; run_realtime.sh for the 1M/s showcase.
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
--trades_processes 1 --market_data_processes 1 --core_processes 1 \
--market_data_min_eps 1200 --market_data_max_eps 2500 \
--core_min_eps 700 --core_max_eps 1000 \
--orders_min_per_sec 5 --orders_max_per_sec 30 \
--min_levels 5 --max_levels 5 \
--total_market_data_events 0 \
--short_ttl true --enterprise true --suffix ${SUFFIX}"
