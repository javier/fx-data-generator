#!/usr/bin/env bash
#
# Real-time FX ingestion over QWP/WebSocket with multi-host HA failover.
# Equivalent data behaviour to the tcp/ILP real-time run, but on QWP: same EPS,
# orders, levels, mode, process count and volume budget; writes the Python table
# names (market_data / core_price / fx_trades).
#
# QWP auth is a single bearer token (--token_file), NOT the tcp JWK x/y coords.
# The --host list gives automatic failover: the client rotates to the writable
# primary and replays each worker's store-and-forward spool on reconnect. List the
# primary FIRST -- DDL/metadata (PG-wire, port 8812) use the first host.
#
# Python has --suffix only (no --prefix; that is a Java concept). It is left empty
# below (bare table names market_data / core_price / fx_trades); override to isolate
# a demo, e.g. --suffix _demo -> market_data_demo.
#
set -euo pipefail

# Python from the venv that has the QWP-capable client (editable questdb 4.1.0).
PY="${PY:-python}"

"$PY" fx_data_generator.py \
  --protocol qwp \
  --host 172.31.42.41:9000,172.31.41.35:9000,10.0.0.8:9000 \
  --qwp_tls true \
  --token_file "$HOME/qwp_token.txt" \
  --durable_ack true \
  --market_data_min_eps 1200 \
  --market_data_max_eps 2500 \
  --core_min_eps 700 \
  --core_max_eps 1000 \
  --orders_min_per_sec 5 \
  --orders_max_per_sec 30 \
  --min_levels 40 \
  --max_levels 40 \
  --mode real-time \
  --processes 1 \
  --total_market_data_events 800_000_000 \
  --create_views false \
  --incremental false \
  --lei_pool_size 2000 \
  --suffix ""
