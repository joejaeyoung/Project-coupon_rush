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
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;

@Service
@RequiredArgsConstructor
public class CouponService {

    private final CouponRepository couponRepository;
    private final IssuedCouponRepository issuedCouponRepository;
    private final CouponRedisService couponRedisService;

    @Transactional(readOnly = true)
    public List<Coupon> getAvailableCoupons() {
        return couponRepository.findByStatus(CouponStatus.ACTIVE);
    }

    /**
     * 선착순 쿠폰 발급.
     *
     *   1. 기간 검증 — Redis (DB 접근 없음)
     *   2. 중복 확인 + 재고 차감 + 발급 이벤트 Stream 기록 — Lua 스크립트 한 번 (원자적)
     *
     * 여기서 끝이다. DB 저장은 CouponStreamRelay → Kafka → CouponKafkaConsumer가 비동기로 이어받는다.
     * Phase 3는 Redis 차감 뒤 Kafka 발행이 별도 호출이라 그 사이에 죽으면 "재고는 줄었는데 발행은 안 된" 예약이 남았다.
     * 이번 구조는 예약과 이벤트 기록이 같은 Redis 명령 안에서 끝나므로 그 틈이 없다.
     */
    public void issueCoupon(Long couponId, Long userId) {
        if (userId == null) {
            throw new CouponException(CouponErrorCode.INVALID_REQUEST);
        }
        couponRedisService.validatePeriod(couponId);
        couponRedisService.reserve(couponId, userId, LocalDateTime.now());
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

    /** DB의 totalQuantity·기간으로 Redis 재고와 이벤트 Stream을 초기화한다 */
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
