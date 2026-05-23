#!/bin/bash
# 사용법:
#   ./run-k6.sh                        # 기본값: phase1-concurrent-issue, coupon_id=1
#   ./run-k6.sh 3                      # coupon_id=3
#   ./run-k6.sh 3 phase1-duplicate-check

COUPON_ID=${1:-1}
USERS=${2:-1000}
SCRIPT=${3:-phase1-concurrent-issue}

echo "▶ k6 실행: script=${SCRIPT}.js | coupon_id=${COUPON_ID} | users=${USERS}"

k6 run \
  --out influxdb=http://localhost:8086/k6 \
  -e COUPON_ID=${COUPON_ID} \
  -e USERS=${USERS} \
  k6/${SCRIPT}.js
