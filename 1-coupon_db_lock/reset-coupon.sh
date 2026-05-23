#!/bin/bash
COUPON_ID=${1:-1}

docker exec mysql mysql -u jajo -pjajo coupon_db \
  -e "DELETE FROM issued_coupon WHERE coupon_id=${COUPON_ID};
      UPDATE coupon SET issued_quantity=0 WHERE id=${COUPON_ID};"

echo "✅ 쿠폰 ${COUPON_ID} 초기화 완료 (DB)"
