package com.example.coupon.event;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * Kafka로 전달되는 쿠폰 발급 이벤트 객체.
 *
 * Producer (CouponKafkaProducer)  →  Kafka Topic "coupon-issue"  →  Consumer (CouponKafkaConsumer)
 */
@Getter // Consumer에서 couponId, userId, issuedAt을 꺼낼 수 있게
@NoArgsConstructor  // Kafka JSON 역직렬화 시 기본 생성자가 필요
@AllArgsConstructor // Producer에서 new CouponIssueEvent(couponId, userId, issuedAt)로 생성
public class CouponIssueEvent {
       private Long couponId;
       private Long userId;
       private String issuedAt;
}
