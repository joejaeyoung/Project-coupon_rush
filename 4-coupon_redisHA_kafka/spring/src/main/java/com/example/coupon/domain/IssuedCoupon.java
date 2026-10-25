package com.example.coupon.domain;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/** (coupon_id, user_id) 유니크 제약이 DB 차원의 최종 중복 방어선이다 — Redis 판정과 Consumer 멱등성 검사가 모두 뚫려도 두 번 저장되지 않는다 */
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

    private Long couponId;
    private Long userId;
    private LocalDateTime issuedAt;

    public IssuedCoupon(Long couponId, Long userId, LocalDateTime issuedAt) {
        this.couponId = couponId;
        this.userId = userId;
        this.issuedAt = issuedAt;
    }
}
