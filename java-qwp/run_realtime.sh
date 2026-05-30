#!/usr/bin/env bash
# Real-time qwp_trades ingestion over QWP (WebSocket), HA-aware.
# Edit HOSTS / auth / TLS for your fleet. All hosts share one credential set
# and one transport scheme (ws or wss).
set -euo pipefail
cd "$(dirname "$0")"

HOSTS="${HOSTS:-127.0.0.1:9000}"

mvn -q compile
exec mvn -q exec:java -Dexec.args="\
  --mode real-time \
  --hosts ${HOSTS} \
  --trades-min-per-sec 50 \
  --trades-max-per-sec 200 \
  --total-trades 0 \
  --yahoo-refresh-secs 300"

# HA + TLS + token example (uncomment, replace TOKEN):
#   HOSTS="primary:9000,replica1:9000,replica2:9000" \
#   exec mvn -q exec:java -Dexec.args="--mode real-time --hosts ${HOSTS} \
#     --tls-insecure --token <TOKEN> --total-trades 0"
