#!/usr/bin/env bash
# Faster-than-life backfill / stress test: no wall-clock wait, bounded by
# --total-trades (and/or --end-ts). Continues strictly after the last row.
set -euo pipefail
cd "$(dirname "$0")"

HOSTS="${HOSTS:-127.0.0.1:9000}"
TOTAL="${TOTAL:-1000000}"

mvn -q compile
exec mvn -q exec:java -Dexec.args="\
  --mode faster-than-life \
  --hosts ${HOSTS} \
  --trades-min-per-sec 100 \
  --trades-max-per-sec 500 \
  --total-trades ${TOTAL}"
