#!/bin/bash
set -e

# Build the python command from env vars. Extend as needed.
exec python fx_data_generator.py \
  --mode "${MODE}" \
  --short_ttl "${SHORT_TTL}" \
  --market_data_min_eps "${MARKET_DATA_MIN_EPS}" \
  --market_data_max_eps "${MARKET_DATA_MAX_EPS}" \
  --core_min_eps "${CORE_MIN_EPS}" \
  --core_max_eps "${CORE_MAX_EPS}" \
  --total_market_data_events "${TOTAL_MARKET_DATA_EVENTS}" \
  --protocol "${PROTOCOL}" \
  --host "${HOST}" \
  --user "${PG_USER}" \
  --password "${PG_PASSWORD}" \
  --pg_port "${PG_PORT}" \
  --token "${TOKEN}" \
  --token_x "${TOKEN_X}" \
  --token_y "${TOKEN_Y}" \
  --ilp_user "${ILP_USER}" \
  --yahoo_refresh_secs "${YAHOO_REFRESH_SECS}" \
  --orders_min_per_sec "${ORDERS_MIN_PER_SEC}" \
  --orders_max_per_sec "${ORDERS_MAX_PER_SEC}" \
  --lei_pool_size "${LEI_POOL_SIZE}" \
  "$@"
