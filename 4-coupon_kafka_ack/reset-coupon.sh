#!/bin/bash
COUPON_ID=${1:-1}
TOTAL_QTY=${2:-100}

echo "1) DB 초기화..."
docker exec mysql mysql -u coupon -pcoupon coupon_db -e "
  INSERT INTO coupon (id, name, description, coupon_type, discount_value,
                      max_discount_amount, min_order_amount, total_quantity,
                      issued_quantity, valid_days, status, start_date, end_date)
  VALUES (${COUPON_ID}, '테스트 쿠폰 ${COUPON_ID}', '선착순 쿠폰 테스트용', 'FIXED_AMOUNT', 1000,
          10000, 5000, ${TOTAL_QTY},
          0, 30, 'ACTIVE', NOW(), DATE_ADD(NOW(), INTERVAL 30 DAY))
  ON DUPLICATE KEY UPDATE
    total_quantity = ${TOTAL_QTY},
    issued_quantity = 0,
    status = 'ACTIVE';

  DELETE FROM issued_coupon WHERE coupon_id=${COUPON_ID};
"

echo "2) Redis 초기화..."
docker exec redis redis-cli DEL \
  "coupon:${COUPON_ID}:stock" \
  "coupon:${COUPON_ID}:users" \
  "coupon:${COUPON_ID}:startDate" \
  "coupon:${COUPON_ID}:endDate"

echo "3) Redis 재고 초기화 (API 호출)..."
curl -s -X POST "http://localhost:8080/api/coupons/${COUPON_ID}/stock/init" > /dev/null

echo "✅ 쿠폰 ${COUPON_ID} 초기화 완료 (DB + Redis, total_quantity=${TOTAL_QTY})"
