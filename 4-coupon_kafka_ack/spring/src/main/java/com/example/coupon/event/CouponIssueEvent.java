package com.example.coupon.event;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * 쿠폰 발급 이벤트.
 *
 * CouponKafkaProducer → Kafka "coupon-issue" → CouponKafkaConsumer
 */
@Getter
@NoArgsConstructor  // Kafka JSON 역직렬화용
@AllArgsConstructor
public class CouponIssueEvent {

    private Long couponId;
    private Long userId;
    private String issuedAt;
}
