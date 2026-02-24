#!/usr/bin/env bash
cd "$(dirname "${BASH_SOURCE[0]:-$0}")"
source .env

THREADS=${1:-8}
CONNECTIONS=${2:-20}
DURATION=${3:-60s}

wrk -t"$THREADS" -c"$CONNECTIONS" -d"$DURATION" -s query_bench_fx.lua "$HOST"
