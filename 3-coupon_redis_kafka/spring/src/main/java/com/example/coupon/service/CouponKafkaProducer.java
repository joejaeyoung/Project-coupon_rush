package com.example.coupon.service;

import com.example.coupon.event.CouponIssueEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

/**
 * Kafka Producer — 쿠폰 발급 이벤트를 Topic에 발행한다.
 *
 * [Phase 2와 비교]
 *   Phase 2: issueCoupon → Redis → MySQL INSERT (동기, 성공 후 200 반환)
 *   Phase 3: issueCoupon → Redis → Kafka 발행 (비동기, 발행 직후 202 반환)
 *            MySQL INSERT는 CouponKafkaConsumer가 별도로 처리
 *
 * [왜 couponId를 Kafka Key로 쓰는가]
 *   Kafka는 같은 Key를 가진 메시지를 항상 같은 Partition에 넣는다.
 *   couponId를 Key로 쓰면 "쿠폰 1번의 발급 이벤트"는 모두 Partition 0에 순서대로 쌓인다.
 *   → Consumer가 Partition을 순서대로 읽으면 발급 순서가 보장된다.
 *   → "50번째 발급자에게 특별 이벤트" 같은 순서 기반 비즈니스 로직이 가능해진다.
 */
@Service
@RequiredArgsConstructor
public class CouponKafkaProducer {

    // Kafka 토픽 이름. Consumer의 @KafkaListener(topics = ...) 와 반드시 일치해야 한다.
    static final String TOPIC = "coupon-issue";
    private final KafkaTemplate<String, CouponIssueEvent> kafkaTemplate;

    /**
     * 쿠폰 발급 이벤트를 Kafka에 발행한다.
     *
     * @param couponId 발급할 쿠폰 ID
     * @param userId   발급받는 유저 ID
     */
    public void publish(Long couponId, Long userId) {
        CouponIssueEvent couponIssueEvent = new CouponIssueEvent(couponId, userId, LocalDateTime.now().toString());

        // 이벤트 발행
        kafkaTemplate.send(TOPIC, couponId.toString(), couponIssueEvent);
    }
}
