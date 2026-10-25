package com.example.coupon.domain.issuedcoupon;

import com.example.coupon.domain.coupon.Coupon;
import com.example.coupon.exception.CouponErrorCode;
import com.example.coupon.exception.CouponException;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * 발급된 쿠폰. (coupon_id, user_id) 유니크 제약이 DB 차원의 최종 중복 방어선이다 —
 * Redis 판정과 Consumer 멱등성 검사가 모두 뚫려도 같은 사용자에게 두 번 저장되지 않는다.
 */
@Entity
@Table(
        name = "issued_coupon",
        uniqueConstraints = @UniqueConstraint(name = "uk_issued_coupon_coupon_user", columnNames = {"coupon_id", "user_id"})
)
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class IssuedCoupon {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "coupon_id")
    private Coupon coupon;

    private Long userId;

    @Enumerated(EnumType.STRING)
    private IssuedCouponStatus status;

    private LocalDateTime issuedAt;
    private LocalDateTime expiredAt;
    private LocalDateTime usedAt;       // nullable

    private IssuedCoupon(Coupon coupon, Long userId, LocalDateTime issuedAt) {
        this.coupon = coupon;
        this.userId = userId;
        this.status = IssuedCouponStatus.ISSUED;
        this.issuedAt = issuedAt;
        this.expiredAt = issuedAt.plusDays(coupon.getValidDays());
    }

    public static IssuedCoupon issue(Coupon coupon, Long userId, LocalDateTime issuedAt) {
        return new IssuedCoupon(coupon, userId, issuedAt);
    }

    public void use(LocalDateTime now) {
        if (status != IssuedCouponStatus.ISSUED) {
            throw new CouponException(CouponErrorCode.NOT_AVAILABLE);
        }
        if (expiredAt.isBefore(now)) {
            throw new CouponException(CouponErrorCode.EXPIRED);
        }
        this.status = IssuedCouponStatus.USED;
        this.usedAt = now;
    }
}
