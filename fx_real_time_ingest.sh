#!/usr/bin/env bash
# fx_realtime_ingest.sh
# Real-time (wall-clock) ingest for the fx generator.
# Real-time requires --processes 1 and ignores --start_ts.

# Optional layers, off by default. Uncomment a line to enable it.
EXTRA_ARGS=(
  # --metals true     # adds XAUUSD, XAGUSD, XPTUSD, XPDUSD, XAUEUR; FX pairs get 30/35 of their rows
  # --forwards true   # adds fx_forward_points; the first run needs --create_views true for its _1m view
)

python fx_data_generator.py \
  --host localhost  \
  --market_data_min_eps 1200 \
  --market_data_max_eps 2500 \
  --core_min_eps 700 \
  --core_max_eps 1000 \
  --ilp_user ilp_ingest \
  --protocol tcp \
  --mode real-time \
  --processes 1 \
  --total_market_data_events 800_000_000 \
  --create_views false \
  --incremental false \
  --orders_min_per_sec 5 \
  --orders_max_per_sec 30 \
  --lei_pool_size 2000 \
  ${EXTRA_ARGS[@]+"${EXTRA_ARGS[@]}"}
