package com.example.coupon.service;

import com.example.coupon.exception.CouponErrorCode;
import com.example.coupon.exception.CouponException;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.Optional;

/**
 * Redis 기반 재고·중복 제어. 판정은 여기서 끝나고 DB 저장은 Kafka 뒤로 넘어간다.
 *
 *   재고 카운터 : coupon:{couponId}:stock
 *   발급자 집합 : coupon:{couponId}:users
 *   발급 기간   : coupon:{couponId}:startDate / coupon:{couponId}:endDate
 */
@Service
@RequiredArgsConstructor
public class CouponRedisService {

    private final StringRedisTemplate redisTemplate;

    private String stockKey(Long couponId)     { return "coupon:" + couponId + ":stock"; }
    private String usersKey(Long couponId)     { return "coupon:" + couponId + ":users"; }
    private String startDateKey(Long couponId) { return "coupon:" + couponId + ":startDate"; }
    private String endDateKey(Long couponId)   { return "coupon:" + couponId + ":endDate"; }

    /** 재고·기간을 Redis에 올린다. 발급 핫패스는 이후 DB를 보지 않는다 */
    public void initStock(Long couponId, int totalQuantity, LocalDateTime startDate, LocalDateTime endDate) {
        redisTemplate.opsForValue().set(stockKey(couponId), String.valueOf(totalQuantity));
        redisTemplate.opsForValue().set(startDateKey(couponId), startDate.toString());
        redisTemplate.opsForValue().set(endDateKey(couponId), endDate.toString());
    }

    /** 발급 기간 검증. 키가 없으면 initStock 미호출이므로 NOT_FOUND */
    public void validatePeriod(Long couponId) {
        String start = redisTemplate.opsForValue().get(startDateKey(couponId));
        String end = redisTemplate.opsForValue().get(endDateKey(couponId));
        if (start == null || end == null) {
            throw new CouponException(CouponErrorCode.NOT_FOUND);
        }
        LocalDateTime now = LocalDateTime.now();
        if (LocalDateTime.parse(start).isAfter(now) || LocalDateTime.parse(end).isBefore(now)) {
            throw new CouponException(CouponErrorCode.NOT_AVAILABLE);
        }
    }

    /** 잔여 재고. 키가 없으면 empty — 호출 측에서 DB fallback */
    public Optional<Integer> getStock(Long couponId) {
        return Optional.ofNullable(redisTemplate.opsForValue().get(stockKey(couponId)))
                .map(Integer::parseInt);
    }

    /**
     * 중복 확인(SADD) → 재고 차감(DECR).
     *
     * 명령 각각은 원자적이지만 둘 사이는 아니다. 재고가 음수가 되면 INCR·SREM으로 되돌린다.
     * SADD 뒤 DECR 전에 프로세스가 죽으면 그 사용자만 users에 남는다(재고는 그대로).
     * 이 창을 닫으려면 Lua나 outbox가 필요한데, 이 단계에서는 복잡도를 이유로 두지 않는다 (README 참고).
     */
    public void checkAndIssue(Long couponId, Long userId) {
        Long added = redisTemplate.opsForSet().add(usersKey(couponId), String.valueOf(userId));
        if (added == null || added == 0) {
            throw new CouponException(CouponErrorCode.DUPLICATED);
        }
        Long remaining = redisTemplate.opsForValue().decrement(stockKey(couponId));
        if (remaining == null || remaining < 0) {
            rollback(couponId, userId);
            throw new CouponException(CouponErrorCode.EXHAUSTED);
        }
    }

    /** Redis 판정 이후 단계(Kafka 발행)가 실패했을 때 예약을 되돌린다: INCR + SREM */
    public void rollback(Long couponId, Long userId) {
        redisTemplate.opsForValue().increment(stockKey(couponId));
        redisTemplate.opsForSet().remove(usersKey(couponId), String.valueOf(userId));
    }
}
