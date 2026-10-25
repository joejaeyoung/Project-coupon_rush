#!/bin/bash
COUPON_ID=${1:-1}
TOTAL_QTY=${2:-100}
# Sentinel에게 현재 primary 포트를 묻는다 (failover 뒤에는 6382일 수 있다). Redis 묶음은 네임스페이스를 공유하므로 sentinel-1에서 127.0.0.1:<port>로 닿는다
redis() { docker exec sentinel-1 redis-cli -p "$(docker exec sentinel-1 redis-cli -p 26379 SENTINEL get-master-addr-by-name mymaster | tail -1)" "$@"; }

echo "1) DB 초기화..."
docker exec mysql mysql -u coupon -pcoupon coupon_db -e "
  INSERT INTO coupon (id, total_quantity, issued_quantity, start_date, end_date)
  VALUES (${COUPON_ID}, ${TOTAL_QTY}, 0, NOW(), DATE_ADD(NOW(), INTERVAL 30 DAY))
  ON DUPLICATE KEY UPDATE total_quantity = ${TOTAL_QTY}, issued_quantity = 0;
  DELETE FROM issued_coupon WHERE coupon_id=${COUPON_ID};
"

echo "2) Redis 초기화..."
redis DEL \
  "coupon:${COUPON_ID}:stock" "coupon:${COUPON_ID}:users" \
  "coupon:${COUPON_ID}:startDate" "coupon:${COUPON_ID}:endDate"

echo "3) Redis 재고 초기화 (API 호출)..."
curl -s -X POST "http://localhost:8080/api/coupons/${COUPON_ID}/stock/init" > /dev/null

echo "✅ 쿠폰 ${COUPON_ID} 초기화 완료 (DB + Redis, total_quantity=${TOTAL_QTY})"
