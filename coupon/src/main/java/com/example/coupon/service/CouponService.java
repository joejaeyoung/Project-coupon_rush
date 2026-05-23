package com.example.coupon.service;

import com.example.coupon.domain.coupon.Coupon;
import com.example.coupon.domain.coupon.Status;
import com.example.coupon.domain.issuedCoupon.IssuedCoupon;
import com.example.coupon.exception.CouponErrorCode;
import com.example.coupon.exception.CouponException;
import com.example.coupon.repository.CouponRepository;
import com.example.coupon.repository.IssuedCouponRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

@Service
@RequiredArgsConstructor
public class CouponService {

    private final CouponRepository couponRepository;
    private final IssuedCouponRepository issuedCouponRepository;

    // ─────────────────────────────────────────────────────────
    // 발급 가능한 쿠폰 목록 조회
    // ─────────────────────────────────────────────────────────
    @Transactional(readOnly = true)
    public Object getAllAvailableCoupons() {
        List<Coupon> ret = couponRepository.findByStatus(Status.ACTIVE);
        return ret;
    }

    // ─────────────────────────────────────────────────────────
    // 선착순 쿠폰 발급 (핵심 로직 — 동시성 제어 여기서 발생)
    // ─────────────────────────────────────────────────────────
    @Transactional
    public void issueCoupon(Long couponId, Long userId) {
        Optional<Coupon> coupon = couponRepository.findByIdWithLock(couponId);

        // 1. 없으면 에러 처리
        if (coupon.isPresent()) {
            throw new CouponException(CouponErrorCode.NOT_FOUND);
        }

        // 2. 발급 기간 체크
        LocalDateTime now = LocalDateTime.now();
        if (coupon.get().getStartDate().isAfter(now) || coupon.get().getEndDate().isBefore(now)) {
            throw new CouponException(CouponErrorCode.NOT_AVAILABLE);
        }

        // 3. 재고 체크
        if (coupon.get().getIssuedQuantity() > coupon.get().getTotalQuantity()) {
            throw new CouponException(CouponErrorCode.EXHAUSTED);
        }

        // 4. 중복 발급
        if (issuedCouponRepository.existsByCoupon_IdAndUserId(couponId, userId)) {
            throw new CouponException(CouponErrorCode.DUPLICATED);
        }

        // 5. 발급 수량 증가
        // Transactional로 저장 처리
        coupon.get().setIssuedQuantity(coupon.get().getIssuedQuantity() + 1);

        // 6. issuedCoupon 생성
        IssuedCoupon issuedCoupon = new IssuedCoupon();
        issuedCoupon.setCoupon(coupon.get());
        issuedCoupon.setUserId(userId);
        issuedCoupon.setStatus(com.example.coupon.domain.issuedCoupon.Status.ISSUED);
        issuedCoupon.setIssuedAt(now);
        issuedCoupon.setExpiredAt(now.plusDays(coupon.get().getValidDays()));
        issuedCouponRepository.save(issuedCoupon);
    }

    // ─────────────────────────────────────────────────────────
    // 잔여 수량 조회
    // ─────────────────────────────────────────────────────────
    @Transactional(readOnly = true)
    public Object getStock(Long couponId) {
        Optional<Coupon> coupon = couponRepository.findById(couponId);
        if (coupon.isEmpty()) {
            throw new CouponException(CouponErrorCode.NOT_FOUND);
        }

        return coupon.get().getTotalQuantity() - coupon.get().getIssuedQuantity();
    }

    // ─────────────────────────────────────────────────────────
    // 사용자 보유 쿠폰 전체 목록
    // ─────────────────────────────────────────────────────────
    @Transactional(readOnly = true)
    public Object getUserCoupons(Long userId) {
        return issuedCouponRepository.findByUserId(userId);
    }

    // ─────────────────────────────────────────────────────────
    // 사용 가능한 쿠폰 목록 (status = ISSUED)
    // ─────────────────────────────────────────────────────────
    @Transactional(readOnly = true)
    public Object getUserUsableCoupons(Long userId) {
        return issuedCouponRepository.findByUserIdAndStatus(userId, com.example.coupon.domain.issuedCoupon.Status.ISSUED);
    }

    // ─────────────────────────────────────────────────────────
    // 쿠폰 사용 처리
    // ─────────────────────────────────────────────────────────
    @Transactional
    public void useCoupon(Long issuedCouponId) {
        Optional<IssuedCoupon> coupon = issuedCouponRepository.findById(issuedCouponId);

        // 1. 쿠폰 확인
        if (coupon.isEmpty()) {
            throw new CouponException(CouponErrorCode.NOT_FOUND);
        }

        // 2. 사용 가능 status 확인
        if (coupon.get().getStatus() != com.example.coupon.domain.issuedCoupon.Status.ISSUED) {
            throw new CouponException(CouponErrorCode.NOT_AVAILABLE);
        }

        // 3. 만료 여부 확인
        if (coupon.get().getExpiredAt().isBefore(LocalDateTime.now())) {
            throw new CouponException(CouponErrorCode.EXPIRATION);
        }

        // 4. 상태 변경
        coupon.get().setStatus(com.example.coupon.domain.issuedCoupon.Status.USED);
        coupon.get().setUsedAt(LocalDateTime.now());
    }
}
