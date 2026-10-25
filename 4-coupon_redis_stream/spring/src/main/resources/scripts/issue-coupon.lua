-- 선착순 쿠폰 예약: 중복 확인 → 재고 차감 → 발급 이벤트 Stream 기록을 한 번에 실행한다.
-- Redis는 Lua 스크립트를 단일 명령처럼 실행하므로 세 단계 사이에 다른 요청이 끼어들 수 없고,
-- 중간에 끊겨 "재고는 줄었는데 이벤트는 없는" 상태도 생기지 않는다.
--
-- KEYS[1] = coupon:{couponId}:stock   재고 카운터
-- KEYS[2] = coupon:{couponId}:users   발급자 집합
-- KEYS[3] = coupon:{couponId}:events  발급 이벤트 Stream
-- ARGV[1] = couponId
-- ARGV[2] = userId
-- ARGV[3] = issuedAt (ISO-8601)
--
-- 반환: 0 이상 = 예약 후 남은 재고 / -1 = 중복 발급 / -2 = 재고 소진 / -3 = 재고 미초기화

if redis.call('SISMEMBER', KEYS[2], ARGV[2]) == 1 then
  return -1
end

local stock = tonumber(redis.call('GET', KEYS[1]))
if stock == nil then
  return -3
end
if stock <= 0 then
  return -2
end

redis.call('DECR', KEYS[1])
redis.call('SADD', KEYS[2], ARGV[2])
redis.call('XADD', KEYS[3], '*', 'couponId', ARGV[1], 'userId', ARGV[2], 'issuedAt', ARGV[3])

return stock - 1
