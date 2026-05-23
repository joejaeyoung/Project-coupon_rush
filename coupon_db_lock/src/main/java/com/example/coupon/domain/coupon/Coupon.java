package com.example.coupon.domain.coupon;


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
public class Coupon {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String name;                //쿠폰명
    private String description;         //설명

    @Enumerated(EnumType.STRING)
    private CouponType couponType;      //쿠폰 타입

    @Enumerated(EnumType.STRING)
    private Status status;              //쿠폰 상태

    private Integer discountValue;      //할인 값

    private Integer minOrderAmount;     //최소 주문 금액 (nullable)
    private Integer maxDiscountAmount;  //최대 할인 금액 (nullable)

    private Integer totalQuantity;      //총 발급 수량
    private Integer issuedQuantity;     //현재 발급 수량

    private Integer validDays;          //발급 후 유효 일수
    private LocalDateTime startDate;    //발급 시작일
    private LocalDateTime endDate;      //발급 종료 일
}
