#!/usr/bin/env bash
#
# Faster-than-life (batch) FX backfill over QWP/WebSocket with multi-host HA failover.
# The QWP counterpart of fx_backfill_ingest.sh, with the same connection settings as
# fx_qwp_real_time.sh: a single bearer token (--token_file), TLS, durable acks, and a --host
# list with the primary FIRST (DDL/metadata over PG-wire, port 8812, use the first host).
#
# Backfills [--start_ts, --end_ts) with --processes workers. Run fx_qwp_real_time.sh
# afterwards to continue live on the same tables: it starts after the latest row.
#
set -euo pipefail

# Python from the venv that has the QWP-capable client.
PY="${PY:-python}"

# Optional layers, off by default. Uncomment a line to enable it.
EXTRA_ARGS=(
  # --metals true     # adds XAUUSD, XAGUSD, XPTUSD, XPDUSD, XAUEUR; FX pairs get 30/35 of their rows
  # --forwards true   # adds fx_forward_points; the first run needs --create_views true for its _1m view
)

"$PY" fx_data_generator.py \
  --protocol qwp \
  --host 172.31.42.41:9000,172.31.41.35:9000,10.0.0.8:9000 \
  --qwp_tls true \
  --token_file "$HOME/qwp_token.txt" \
  --durable_ack true \
  --market_data_min_eps 8200 \
  --market_data_max_eps 11000 \
  --core_min_eps 700 \
  --core_max_eps 1000 \
  --orders_min_per_sec 5 \
  --orders_max_per_sec 30 \
  --min_levels 40 \
  --max_levels 40 \
  --mode faster-than-life \
  --processes 6 \
  --chunk_seconds 300 \
  --total_market_data_events 600_000_000 \
  --start_ts "2026-10-12T00:00:00.000000Z" \
  --end_ts "2026-10-12T14:00:00.000000Z" \
  --create_views false \
  --incremental false \
  --lei_pool_size 2000 \
  --suffix "" \
  ${EXTRA_ARGS[@]+"${EXTRA_ARGS[@]}"}
