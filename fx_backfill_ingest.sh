#!/usr/bin/env bash
# fx_backfill_ingest.sh
# Batch (faster-than-life) backfill for the fx generator.

python fx_data_generator.py \
  --host 172.31.42.41 \
  --market_data_min_eps 8200 \
  --market_data_max_eps 11000 \
  --core_min_eps 700 \
  --core_max_eps 1000 \
  --token "REPLACE_ME_token" \
  --token_x "REPLACE_ME_token_x" \
  --token_y "REPLACE_ME_token_y" \
  --ilp_user ilp_ingest \
  --protocol tcp \
  --mode faster-than-life \
  --processes 6 \
  --total_market_data_events 600_000_000 \
  --start_ts "2025-11-11T00:00:00.000000Z" \
  --end_ts "2025-11-11T14:00:00.000000Z" \
  --min_levels 3 \
  --max_levels 3 \
  --create_views false \
  --short_ttl false \
  --incremental false \
  --orders_min_per_sec 5 \
  --orders_max_per_sec 30 \
  --lei_pool_size 2000
