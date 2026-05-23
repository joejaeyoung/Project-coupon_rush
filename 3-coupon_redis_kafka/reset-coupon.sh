#!/bin/bash
COUPON_ID=${1:-1}

echo "1) DB 초기화..."
docker exec mysql mysql -u jajo -pjajo coupon_db \
  -e "DELETE FROM issued_coupon WHERE coupon_id=${COUPON_ID};
      UPDATE coupon SET issued_quantity=0 WHERE id=${COUPON_ID};"

echo "2) Redis 초기화..."
docker exec redis redis-cli DEL \
  "coupon:${COUPON_ID}:stock" \
  "coupon:${COUPON_ID}:users" \
  "coupon:${COUPON_ID}:startDate" \
  "coupon:${COUPON_ID}:endDate"

echo "3) Redis 재고 초기화..."
curl -s -X POST "http://localhost:8080/api/coupons/${COUPON_ID}/stock/init" > /dev/null

echo "✅ 쿠폰 ${COUPON_ID} 초기화 완료 (DB + Redis)"
