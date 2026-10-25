package com.example.coupon.service;

import com.example.coupon.event.CouponIssueEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

/**
 * Kafka Producer — 발급 이벤트를 "coupon-issue" 토픽에 발행한다.
 *
 * 브로커 ACK(acks=all)를 받을 때까지 기다린다. 포기 시점은 producer 설정이 정한다 —
 * send() 자체가 max.block.ms 안에 메타데이터·버퍼를 못 얻으면 동기 예외(TimeoutException), 그 뒤 delivery.timeout.ms 안에 ACK가 없으면
 * join()이 CompletionException을 던진다. 둘 다 RuntimeException이라 CouponService가 한 catch로 받아 예약을 되돌린다.
 * couponId를 Key로 쓰므로 같은 쿠폰의 이벤트는 같은 파티션에 발행 순서대로 쌓인다.
 */
@Component
@RequiredArgsConstructor
public class CouponKafkaProducer {

    static final String TOPIC = "coupon-issue";

    private final KafkaTemplate<String, CouponIssueEvent> kafkaTemplate;

    public void publish(Long couponId, Long userId) {
        CouponIssueEvent event = new CouponIssueEvent(couponId, userId, LocalDateTime.now());
        kafkaTemplate.send(TOPIC, String.valueOf(couponId), event).join();
    }
}
