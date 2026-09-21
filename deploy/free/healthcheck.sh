#!/usr/bin/env bash
set -euo pipefail

curl --fail --silent --show-error http://127.0.0.1:8100/api/charts/exchanges
echo
curl --fail --silent --show-error http://127.0.0.1:8100/api/listings/meta
echo
curl --fail --silent --show-error http://127.0.0.1:8080/charts >/dev/null
systemctl --no-pager --full status \
  free-screener-redis.service \
  free-screener-web.service \
  free-screener-gateway.service \
  free-screener-ingest@klines-core.service \
  free-screener-ingest@klines-alt.service

