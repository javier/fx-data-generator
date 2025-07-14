#!/bin/bash
set -e

# Build the python command from env vars. Extend as needed.
exec python fx_data_generator.py \
  --mode "${MODE}" \
  --market_data_min_eps "${MARKET_DATA_MIN_EPS}" \
  --market_data_max_eps "${MARKET_DATA_MAX_EPS}" \
  --core_min_eps "${CORE_MIN_EPS}" \
  --core_max_eps "${CORE_MAX_EPS}" \
  --protocol "${PROTOCOL}" \
  --host "${HOST}" \
  --user "${PG_USER}" \
  --password "${PG_PASSWORD}" \
  --pg_port "${PG_PORT}" \
  --token "${TOKEN}" \
  --token_x "${TOKEN_X}" \
  --token_y "${TOKEN_Y}" \
  --ilp_user "${ILP_USER}" \
  "$@"
