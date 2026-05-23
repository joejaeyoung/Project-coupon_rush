package com.example.coupon.repository;

import com.example.coupon.domain.issuedCoupon.IssuedCoupon;
import com.example.coupon.domain.issuedCoupon.Status;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface IssuedCouponRepository extends JpaRepository<IssuedCoupon, Long> {

    // ─────────────────────────────────────────────────────────
    // 사용자 보유 쿠폰 전체 조회
    // ─────────────────────────────────────────────────────────
    List<IssuedCoupon> findByUserId(Long userId);

    // ─────────────────────────────────────────────────────────
    // 사용 가능한 쿠폰 조회 (status = ISSUED)
    // ─────────────────────────────────────────────────────────
    List<IssuedCoupon> findByUserIdAndStatus(Long userId, Status status);

    // ─────────────────────────────────────────────────────────
    // 중복 발급 체크: 해당 쿠폰을 해당 유저가 이미 발급받았는지 확인
    // ─────────────────────────────────────────────────────────
    boolean existsByCoupon_IdAndUserId(Long couponId, Long userId);
}
