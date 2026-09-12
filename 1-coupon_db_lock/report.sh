#!/bin/bash
COUPON_ID=${1:-1}

echo "========================================"
echo " 쿠폰 #${COUPON_ID} 발급 현황 리포트"
echo "========================================"

# ── MySQL ───────────────────────────────────
DB_ISSUED_COUNT=$(docker exec mysql mysql -u coupon -pcoupon coupon_db \
  --skip-column-names -se \
  "SELECT COUNT(*) FROM issued_coupon WHERE coupon_id=${COUPON_ID};" 2>/dev/null)

DB_ISSUED_QUANTITY=$(docker exec mysql mysql -u coupon -pcoupon coupon_db \
  --skip-column-names -se \
  "SELECT issued_quantity FROM coupon WHERE id=${COUPON_ID};" 2>/dev/null)

DB_TOTAL_QUANTITY=$(docker exec mysql mysql -u coupon -pcoupon coupon_db \
  --skip-column-names -se \
  "SELECT total_quantity FROM coupon WHERE id=${COUPON_ID};" 2>/dev/null)

echo ""
echo "[MySQL]"
echo "  issued_coupon 행 수   : ${DB_ISSUED_COUNT:-없음}"
echo "  coupon.issued_quantity: ${DB_ISSUED_QUANTITY:-없음}"
echo "  coupon.total_quantity : ${DB_TOTAL_QUANTITY:-없음}"

# ── 정합성 체크 ─────────────────────────────
echo ""
echo "[정합성]"

if [ -n "$DB_ISSUED_COUNT" ] && [ -n "$DB_ISSUED_QUANTITY" ]; then
  if [ "$DB_ISSUED_COUNT" -eq "$DB_ISSUED_QUANTITY" ]; then
    echo "  DB rows == issued_qty  : ✅ 일치 (${DB_ISSUED_COUNT}건)"
  else
    echo "  DB rows == issued_qty  : ❌ 불일치 (rows=${DB_ISSUED_COUNT} / qty=${DB_ISSUED_QUANTITY})"
  fi
fi

if [ -n "$DB_ISSUED_COUNT" ] && [ -n "$DB_TOTAL_QUANTITY" ]; then
  if [ "$DB_ISSUED_COUNT" -le "$DB_TOTAL_QUANTITY" ]; then
    echo "  초과 발급 없음         : ✅ (${DB_ISSUED_COUNT} / ${DB_TOTAL_QUANTITY})"
  else
    echo "  초과 발급 발생         : ❌ (${DB_ISSUED_COUNT} > ${DB_TOTAL_QUANTITY})"
  fi
fi

echo ""
echo "========================================"
