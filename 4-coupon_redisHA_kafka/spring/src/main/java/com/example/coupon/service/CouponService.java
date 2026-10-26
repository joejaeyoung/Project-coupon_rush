package com.example.coupon.service;

import com.example.coupon.domain.Coupon;
import com.example.coupon.exception.CouponErrorCode;
import com.example.coupon.exception.CouponException;
import com.example.coupon.repository.CouponRepository;
import com.example.coupon.repository.IssuedCouponRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class CouponService {

    private final CouponRepository couponRepository;
    private final IssuedCouponRepository issuedCouponRepository;
    private final CouponRedisService couponRedisService;
    private final CouponKafkaProducer couponKafkaProducer;

    /**
     * 선착순 쿠폰 발급.
     *
     *   1. 기간·소진 선확인 — Redis MGET 한 번 (소진 뒤 요청은 여기서 끝난다)
     *   2. 중복 확인 + 재고 차감 + 복제 수신 확인 — Redis (SADD → DECR → WAIT)
     *   3. Kafka 발행 — 브로커 ACK까지 기다린다. 실패하면 2번을 되돌리고 503.
     *
     * DB 저장은 CouponKafkaConsumer가 비동기로 처리하며 이 메서드는 DB 완료를 기다리지 않는다.
     * ACK 확인에 실패하면 보상을 시도하지만, ACK만 유실된 이벤트는 DB에 저장될 수 있다.
     * Redis 예약·Kafka 발행·보상은 하나의 트랜잭션이 아니므로 프로세스 종료나 보상 실패 시 예약이 남을 수 있다.
     */
    public void issueCoupon(Long couponId, Long userId) {
        couponRedisService.validateIssuable(couponId);
        // 예약 내부의 재고 부족·복제 미확인은 RedisService가 보상한다.
        // 이 호출 자체의 연결 예외는 아래 Kafka 실패 catch에 들어오지 않는다.
        couponRedisService.checkAndIssue(couponId, userId);

        try {
            couponKafkaProducer.publish(couponId, userId);
        } catch (RuntimeException e) {
            // 보상 자체가 실패하면 아래 로그·ISSUE_FAILED 변환 전에 그 예외가 전파된다.
            couponRedisService.rollback(couponId, userId);
            log.warn("Kafka 발행 실패로 예약 되돌림 — couponId={}, userId={}", couponId, userId, e);
            throw new CouponException(CouponErrorCode.ISSUE_FAILED, e);
        }
    }

    public int getStock(Long couponId) {
        return couponRedisService.getStock(couponId);
    }

    /** 발급 요청과 별개의 읽기 경로. Redis 예약 대신 MySQL 커밋 결과를 기준으로 답한다. */
    public boolean isIssued(Long couponId, Long userId) {
        return issuedCouponRepository.existsByCouponIdAndUserId(couponId, userId);
    }

    /**
     * DB 총량·기간을 Redis에 덮어쓴다. 발급 요청 경로에서는 DB를 조회하지 않는다.
     * 이미 발급한 수량을 빼거나 users를 비우지 않으므로, 진행 중인 쿠폰을 복구하는 용도가 아니다.
     */
    public void initStock(Long couponId) {
        Coupon coupon = couponRepository.findById(couponId)
                .orElseThrow(() -> new CouponException(CouponErrorCode.NOT_FOUND));

        couponRedisService.initStock(couponId, coupon.getTotalQuantity(), coupon.getStartDate(), coupon.getEndDate());
    }
}
