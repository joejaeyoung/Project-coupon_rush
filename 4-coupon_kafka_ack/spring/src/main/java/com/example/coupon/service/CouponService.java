package com.example.coupon.service;

import com.example.coupon.api.IssuedCouponResponse;
import com.example.coupon.domain.coupon.Coupon;
import com.example.coupon.domain.coupon.CouponStatus;
import com.example.coupon.domain.issuedcoupon.IssuedCoupon;
import com.example.coupon.domain.issuedcoupon.IssuedCouponStatus;
import com.example.coupon.exception.CouponErrorCode;
import com.example.coupon.exception.CouponException;
import com.example.coupon.repository.CouponRepository;
import com.example.coupon.repository.IssuedCouponRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class CouponService {

    private final CouponRepository couponRepository;
    private final IssuedCouponRepository issuedCouponRepository;
    private final CouponRedisService couponRedisService;
    private final CouponKafkaProducer couponKafkaProducer;

    @Transactional(readOnly = true)
    public List<Coupon> getAvailableCoupons() {
        return couponRepository.findByStatus(CouponStatus.ACTIVE);
    }

    /**
     * 선착순 쿠폰 발급.
     *
     *   1. 기간·소진 선확인 — Redis MGET 한 번 (소진 뒤 요청은 여기서 끝난다)
     *   2. 중복 확인 + 재고 차감 — Redis (SADD → DECR)
     *   3. Kafka 발행 — 브로커 ACK까지 기다린다. 실패하면 2번을 되돌리고 503.
     *
     * DB 저장은 CouponKafkaConsumer가 비동기로 처리한다. Phase 3와 달리 "Kafka에 갔는지"를 응답 전에 확정하므로
     * 발행 실패가 재고 누수로 남지 않는다. 비동기 구간은 로그 외에 남는 게 없으니 실패는 반드시 기록한다.
     */
    public void issueCoupon(Long couponId, long userId) {
        couponRedisService.validateIssuable(couponId);
        couponRedisService.checkAndIssue(couponId, userId);
        try {
            couponKafkaProducer.publish(couponId, userId);
        } catch (RuntimeException e) {
            couponRedisService.rollback(couponId, userId);
            log.warn("Kafka 발행 실패로 예약 되돌림 — couponId={}, userId={}", couponId, userId, e);
            throw new CouponException(CouponErrorCode.ISSUE_FAILED, e);
        }
    }

    /** 잔여 수량 — Redis 우선, 키가 없으면 DB로 fallback (Cache-aside) */
    @Transactional(readOnly = true)
    public int getStock(Long couponId) {
        return couponRedisService.getStock(couponId)
                .orElseGet(() -> findCoupon(couponId).getRemainingQuantity());
    }

    @Transactional(readOnly = true)
    public List<IssuedCouponResponse> getUserCoupons(Long userId) {
        return toResponse(issuedCouponRepository.findByUserId(userId));
    }

    @Transactional(readOnly = true)
    public List<IssuedCouponResponse> getUserUsableCoupons(Long userId) {
        return toResponse(issuedCouponRepository.findByUserIdAndStatus(userId, IssuedCouponStatus.ISSUED));
    }

    /** DB의 totalQuantity·기간으로 Redis 재고를 초기화한다 */
    @Transactional(readOnly = true)
    public void initStock(Long couponId) {
        Coupon coupon = findCoupon(couponId);
        couponRedisService.initStock(couponId, coupon.getTotalQuantity(), coupon.getStartDate(), coupon.getEndDate());
    }

    @Transactional
    public void useCoupon(Long issuedCouponId) {
        IssuedCoupon issuedCoupon = issuedCouponRepository.findById(issuedCouponId)
                .orElseThrow(() -> new CouponException(CouponErrorCode.NOT_FOUND));
        issuedCoupon.use(LocalDateTime.now());
    }

    private Coupon findCoupon(Long couponId) {
        return couponRepository.findById(couponId)
                .orElseThrow(() -> new CouponException(CouponErrorCode.NOT_FOUND));
    }

    private List<IssuedCouponResponse> toResponse(List<IssuedCoupon> issuedCoupons) {
        return issuedCoupons.stream().map(IssuedCouponResponse::from).toList();
    }
}
