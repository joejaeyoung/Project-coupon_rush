package com.example.coupon.event;

import java.time.LocalDateTime;

/**
 * 쿠폰 발급 이벤트. CouponKafkaProducer → Kafka "coupon-issue" → CouponKafkaConsumer
 * spring-kafka의 JsonSerializer/JsonDeserializer가 record와 LocalDateTime을 그대로 다룬다.
 */
public record CouponIssueEvent(Long couponId, Long userId, LocalDateTime issuedAt) {
}
