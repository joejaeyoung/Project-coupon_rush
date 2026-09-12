package com.example.coupon.service;

import com.example.coupon.event.CouponIssueEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
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
 *   파티션 구성을 유지하는 동안 같은 couponId는 같은 파티션으로 라우팅된다.
 *   파티션 번호는 키 해시로 결정되며, HTTP 요청의 도착 순서 전체를 보장하지 않는다.
 */
@Service
@RequiredArgsConstructor
public class CouponKafkaProducer {

    @Value("${coupon.kafka.topic:coupon-issue}")
    private String topic;
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
        kafkaTemplate.send(topic, couponId.toString(), couponIssueEvent);
    }
}
