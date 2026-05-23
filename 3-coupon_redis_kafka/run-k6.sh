#!/bin/bash
COUPON_ID=${1:-1}
USERS=${2:-1000}

echo "▶ k6 실행: coupon_id=${COUPON_ID} | users=${USERS}"

k6 run \
  --out influxdb=http://localhost:8086/k6 \
  -e COUPON_ID=${COUPON_ID} \
  -e USERS=${USERS} \
  k6/concurrent-issue.js
