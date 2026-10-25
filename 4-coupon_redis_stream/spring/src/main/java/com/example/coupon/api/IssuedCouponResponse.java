package com.example.coupon.api;

import com.example.coupon.domain.issuedcoupon.IssuedCoupon;
import com.example.coupon.domain.issuedcoupon.IssuedCouponStatus;

import java.time.LocalDateTime;

public record IssuedCouponResponse(
        Long id,
        Long couponId,
        String couponName,
        Long userId,
        IssuedCouponStatus status,
        LocalDateTime issuedAt,
        LocalDateTime expiredAt,
        LocalDateTime usedAt
) {

    public static IssuedCouponResponse from(IssuedCoupon issuedCoupon) {
        return new IssuedCouponResponse(
                issuedCoupon.getId(),
                issuedCoupon.getCoupon().getId(),
                issuedCoupon.getCoupon().getName(),
                issuedCoupon.getUserId(),
                issuedCoupon.getStatus(),
                issuedCoupon.getIssuedAt(),
                issuedCoupon.getExpiredAt(),
                issuedCoupon.getUsedAt()
        );
    }
}
