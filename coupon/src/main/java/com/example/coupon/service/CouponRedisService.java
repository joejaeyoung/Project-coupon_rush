package com.example.coupon.service;

import com.example.coupon.exception.CouponErrorCode;
import com.example.coupon.exception.CouponException;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;
import java.time.LocalDateTime;

/**
 * Phase 2: Redis 기반 재고/중복 제어
 *
 * Phase 1의 DB Lock 역할을 Redis 원자적 연산으로 대체한다.
 * - 재고 차감: DECR (원자적, DB Lock 없음)
 * - 중복 방지: SADD (원자적, 반환값으로 중복 여부 판단)
 *
 * 이 클래스를 분리한 이유:
 * Phase 3에서 Kafka를 붙이거나, Redisson으로 교체할 때
 * CouponService를 건드리지 않고 이 클래스만 바꾸면 된다.
 */
@Service
@RequiredArgsConstructor
public class CouponRedisService {

    private final RedisTemplate<String, String> redisTemplate;

    // ─────────────────────────────────────────────────────────
    // Redis Key 네이밍 규칙
    //   재고 카운터: "coupon:{couponId}:stock"
    //   발급자 집합: "coupon:{couponId}:users"
    // ─────────────────────────────────────────────────────────

    private String stockKey(Long couponId)     { return "coupon:" + couponId + ":stock"; }
    private String usersKey(Long couponId)     { return "coupon:" + couponId + ":users"; }
    private String startDateKey(Long couponId) { return "coupon:" + couponId + ":startDate"; }
    private String endDateKey(Long couponId)   { return "coupon:" + couponId + ":endDate"; }

    // ─────────────────────────────────────────────────────────
    // 재고 + 기간 초기화
    //   재고 수량과 유효 기간을 Redis에 올린다
    //   → issueCoupon 핫패스에서 DB 조회 없이 Redis만으로 기간 검증 가능
    // ─────────────────────────────────────────────────────────
    public void initStock(Long couponId, int totalQuantity, LocalDateTime startDate, LocalDateTime endDate) {
        redisTemplate.opsForValue().set(stockKey(couponId),     String.valueOf(totalQuantity));
        redisTemplate.opsForValue().set(startDateKey(couponId), startDate.toString());
        redisTemplate.opsForValue().set(endDateKey(couponId),   endDate.toString());
    }

    // ─────────────────────────────────────────────────────────
    // 쿠폰 기간 검증 (Redis)
    //   키가 없으면 NOT_FOUND (initStock 미호출)
    //   기간 외면 NOT_AVAILABLE
    // ─────────────────────────────────────────────────────────
    public void validatePeriod(Long couponId) {
        String startStr = redisTemplate.opsForValue().get(startDateKey(couponId));
        String endStr   = redisTemplate.opsForValue().get(endDateKey(couponId));
        if (startStr == null || endStr == null) {
            throw new CouponException(CouponErrorCode.NOT_FOUND);
        }
        LocalDateTime now = LocalDateTime.now();
        if (LocalDateTime.parse(startStr).isAfter(now) || LocalDateTime.parse(endStr).isBefore(now)) {
            throw new CouponException(CouponErrorCode.NOT_AVAILABLE);
        }
    }

    // ─────────────────────────────────────────────────────────
    // 재고 조회
    //   Redis에서 현재 남은 재고를 반환한다
    //   Redis에 키가 없으면 -1 반환 (호출하는 쪽에서 DB 조회 fallback)
    //
    // 사용할 Redis 명령어: GET
    //   redisTemplate.opsForValue().get(key)  → String 또는 null (-1)
    // ─────────────────────────────────────────────────────────
    public int getStock(Long couponId) {
        String value = redisTemplate.opsForValue().get(stockKey(couponId));
        if (value == null) {
            return -1;
        }
        return Integer.parseInt(value);
    }

    // ─────────────────────────────────────────────────────────
    // 중복 체크 + 재고 차감 (핵심 메서드)
    //
    // 순서가 중요하다:
    //   1단계: SADD로 중복 체크
    //   2단계: DECR로 재고 차감
    //   실패 시: 롤백 (SREM or INCR)
    //
    // ── 1단계: 중복 체크 ──────────────────────────────────────
    // 사용할 Redis 명령어: SADD
    //   redisTemplate.opsForSet().add(key, value)
    //   반환값: 1 → 새로 추가됨(첫 발급), 0 → 이미 있음(중복)
    // ── 2단계: 재고 차감 ──────────────────────────────────────
    // 사용할 Redis 명령어: DECR
    //   redisTemplate.opsForValue().decrement(key)
    //   반환값: 차감 후 남은 재고 수 (Long)
    //           음수가 되면 재고 초과
    // ─────────────────────────────────────────────────────────
    public void checkAndIssue(Long couponId, Long userId) {
        // 1단계 — 중복 체크
        Long cnt = redisTemplate.opsForSet().add(usersKey(couponId), String.valueOf(userId));
        if (cnt == null || cnt == 0L) {
            throw new CouponException(CouponErrorCode.DUPLICATED);
        }

        // 2단계 — 재고 차감
        Long leftCouponCnt = redisTemplate.opsForValue().decrement(stockKey(couponId));
        if (leftCouponCnt == null || leftCouponCnt < 0L) {
            redisTemplate.opsForValue().increment(stockKey(couponId));
            redisTemplate.opsForSet().remove(usersKey(couponId), String.valueOf(userId));
            throw new CouponException(CouponErrorCode.EXHAUSTED);
        }
    }

    // ─────────────────────────────────────────────────────────
    // DB 쓰기 실패 시 Redis 보상 처리 (Compensation)
    //
    // checkAndIssue()가 성공했지만 이후 DB INSERT/UPDATE가 실패할 경우
    // Spring @Transactional이 DB는 자동 롤백하지만 Redis는 롤백하지 않는다.
    // 이 메서드로 Redis 상태를 수동으로 되돌린다.
    //
    //   INCR stockKey     — DECR로 차감한 재고 복구
    //   SREM usersKey     — SADD로 추가한 userId 제거
    // ─────────────────────────────────────────────────────────
    public void rollback(Long couponId, Long userId) {
        redisTemplate.opsForValue().increment(stockKey(couponId));
        redisTemplate.opsForSet().remove(usersKey(couponId), String.valueOf(userId));
    }
}
