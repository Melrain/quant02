#!/usr/bin/env bash
set -euo pipefail

# 配置环境变量
PROM_URL="http://localhost:9090"
REDIS_URL="redis://127.0.0.1:6379/0"
SYMS="BTC-USDT-SWAP,ETH-USDT-SWAP"
DIRS="buy"
SRCS="manual"

cd "$(dirname "$0")/.."

echo "=== Smoke cron run $(date -u +"%Y-%m-%d %H:%M:%S") UTC ==="

PROM_URL=$PROM_URL \
REDIS_URL=$REDIS_URL \
SYMS=$SYMS \
DIRS=$DIRS \
SRCS=$SRCS \
npm run sim:smoke

STATUS=$?
if [ $STATUS -ne 0 ]; then
  echo "❌ Smoke failed at $(date -u)"
else
  echo "✅ Smoke passed at $(date -u)"
fi

exit $STATUS