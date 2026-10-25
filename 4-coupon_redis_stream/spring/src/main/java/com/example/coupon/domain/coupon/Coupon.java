package com.example.coupon.domain.coupon;

import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Entity
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class Coupon {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String name;
    private String description;

    @Enumerated(EnumType.STRING)
    private CouponType couponType;

    @Enumerated(EnumType.STRING)
    private CouponStatus status;

    private Integer discountValue;
    private Integer minOrderAmount;     // nullable
    private Integer maxDiscountAmount;  // nullable

    private Integer totalQuantity;      // 총 발급 수량
    private Integer issuedQuantity;     // 현재 발급 수량 (Kafka Consumer가 증가시킨다)

    private Integer validDays;          // 발급 후 유효 일수
    private LocalDateTime startDate;    // 발급 시작일
    private LocalDateTime endDate;      // 발급 종료일

    @Builder
    public Coupon(String name, String description, CouponType couponType, CouponStatus status,
                  Integer discountValue, Integer minOrderAmount, Integer maxDiscountAmount,
                  Integer totalQuantity, Integer issuedQuantity, Integer validDays,
                  LocalDateTime startDate, LocalDateTime endDate) {
        this.name = name;
        this.description = description;
        this.couponType = couponType;
        this.status = status;
        this.discountValue = discountValue;
        this.minOrderAmount = minOrderAmount;
        this.maxDiscountAmount = maxDiscountAmount;
        this.totalQuantity = totalQuantity;
        this.issuedQuantity = issuedQuantity == null ? 0 : issuedQuantity;
        this.validDays = validDays;
        this.startDate = startDate;
        this.endDate = endDate;
    }

    public int getRemainingQuantity() {
        return totalQuantity - issuedQuantity;
    }
}
