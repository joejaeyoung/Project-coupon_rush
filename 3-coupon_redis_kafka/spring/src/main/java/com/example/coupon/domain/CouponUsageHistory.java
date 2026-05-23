package com.example.coupon.domain;

import com.example.coupon.domain.issuedCoupon.IssuedCoupon;
import jakarta.persistence.*;

import java.time.LocalDateTime;

@Entity
@Table(name = "coupon_usage_history")
public class CouponUsageHistory {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "issued_coupon_id")
    private IssuedCoupon issuedCoupon;

    private Long userId;
    private Long orderId;
    private Integer discountAmount;
    private LocalDateTime usedAt;
}
