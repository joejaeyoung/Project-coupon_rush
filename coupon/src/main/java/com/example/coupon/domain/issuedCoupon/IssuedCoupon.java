package com.example.coupon.domain.issuedCoupon;

import com.example.coupon.domain.coupon.Coupon;
import com.example.coupon.domain.coupon.CouponType;
import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.LocalDateTime;

@Entity
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class IssuedCoupon {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "coupon_id")
    private Coupon coupon;               //쿠폰

    private Long userId;                //발급 받은 사용자 ID

    @Enumerated(EnumType.STRING)
    private Status status;              //쿠폰 상태

    private LocalDateTime issuedAt;     //발급 일시
    private LocalDateTime expiredAt;    //만료 일시
    private LocalDateTime usedAt;       //사용 일시 (nullable)
}
