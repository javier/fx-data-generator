#!/usr/bin/env bash
# fx_realtime_ingest.sh
# Real-time (wall-clock) ingest for the fx generator.
# Real-time requires --processes 1 and ignores --start_ts.

python fx_data_generator.py \
  --host 172.31.42.41 \
  --market_data_min_eps 1200 \
  --market_data_max_eps 2500 \
  --core_min_eps 700 \
  --core_max_eps 1000 \
  --token "REPLACE_ME_token" \
  --token_x "REPLACE_ME_token_x" \
  --token_y "REPLACE_ME_token_y" \
  --ilp_user ilp_ingest \
  --protocol tcp \
  --mode real-time \
  --processes 1 \
  --total_market_data_events 800_000_000 \
  --create_views false \
  --incremental false
