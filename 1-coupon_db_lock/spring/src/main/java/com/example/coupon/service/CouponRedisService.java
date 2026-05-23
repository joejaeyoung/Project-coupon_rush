package com.example.coupon.service;

import com.example.coupon.exception.CouponErrorCode;
import com.example.coupon.exception.CouponException;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

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
    //
    // TODO: 아래 두 메서드를 완성해라
    // ─────────────────────────────────────────────────────────

    private String stockKey(Long couponId) {
        // TODO: "coupon:{couponId}:stock" 형태의 키 반환
        return null;
    }

    private String usersKey(Long couponId) {
        // TODO: "coupon:{couponId}:users" 형태의 키 반환
        return null;
    }

    // ─────────────────────────────────────────────────────────
    // 재고 초기화
    //   쿠폰 생성 시 또는 테스트 리셋 시 호출
    //   Redis에 재고 수량을 세팅한다
    //
    // 사용할 Redis 명령어: SET
    //   redisTemplate.opsForValue().set(key, value)
    //
    // TODO: stockKey에 totalQuantity 값을 String으로 저장해라
    // ─────────────────────────────────────────────────────────
    public void initStock(Long couponId, int totalQuantity) {
        // TODO
    }

    // ─────────────────────────────────────────────────────────
    // 재고 조회
    //   Redis에서 현재 남은 재고를 반환한다
    //   Redis에 키가 없으면 -1 반환 (호출하는 쪽에서 DB 조회 fallback)
    //
    // 사용할 Redis 명령어: GET
    //   redisTemplate.opsForValue().get(key)  → String 또는 null
    //
    // TODO: stockKey 값을 읽어서 Integer로 파싱해 반환해라
    //       null이면 -1 반환
    // ─────────────────────────────────────────────────────────
    public int getStock(Long couponId) {
        // TODO
        return -1;
    }

    // ─────────────────────────────────────────────────────────
    // 중복 체크 + 재고 차감 (핵심 메서드)
    //
    // 순서가 중요하다:
    //   1단계: SADD로 중복 체크
    //   2단계: DECR로 재고 차감
    //   실패 시: 롤백 (SREM or INCR)
    //
    // 왜 이 순서인가?
    //   DECR 먼저 → 재고는 줄었는데 SADD 실패 → 재고 유실
    //   SADD 먼저 → DECR 실패 → SREM으로 되돌릴 수 있음
    //
    // ── 1단계: 중복 체크 ──────────────────────────────────────
    // 사용할 Redis 명령어: SADD
    //   redisTemplate.opsForSet().add(key, value)
    //   반환값: 1 → 새로 추가됨(첫 발급), 0 → 이미 있음(중복)
    //
    // TODO: usersKey에 userId(String)를 SADD 해라
    //       반환값이 0이면 CouponErrorCode.DUPLICATED 예외 던져라
    //
    // ── 2단계: 재고 차감 ──────────────────────────────────────
    // 사용할 Redis 명령어: DECR
    //   redisTemplate.opsForValue().decrement(key)
    //   반환값: 차감 후 남은 재고 수 (Long)
    //           음수가 되면 재고 초과
    //
    // TODO: stockKey를 DECR 해라
    //       반환값이 null이거나 0 미만이면 재고 소진
    //         → stockKey를 INCR로 되돌려라 (increment)
    //         → usersKey에서 userId를 SREM으로 제거해라
    //         → CouponErrorCode.EXHAUSTED 예외 던져라
    //
    // 사용할 메서드:
    //   redisTemplate.opsForValue().decrement(key)   → Long
    //   redisTemplate.opsForValue().increment(key)   → 롤백용
    //   redisTemplate.opsForSet().remove(key, value) → 롤백용
    // ─────────────────────────────────────────────────────────
    public void checkAndIssue(Long couponId, Long userId) {
        // TODO: 1단계 — 중복 체크

        // TODO: 2단계 — 재고 차감
    }
}
