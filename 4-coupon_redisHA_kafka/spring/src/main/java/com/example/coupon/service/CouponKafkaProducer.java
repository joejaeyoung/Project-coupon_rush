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
 * couponId를 Key로 쓰므로 같은 쿠폰의 이벤트는 같은 파티션으로 간다.
 * Redis 예약 순서와 Kafka 기록 순서는 동시 요청의 스레드 실행 순서에 따라 달라질 수 있다.
 */
@Component
@RequiredArgsConstructor
public class CouponKafkaProducer {

    static final String TOPIC = "coupon-issue";

    private final KafkaTemplate<String, CouponIssueEvent> kafkaTemplate;

    public void publish(Long couponId, Long userId) {
        // issuedAt은 이벤트 생성 시각이다. Consumer가 DB에 커밋한 시각이 아니다.
        CouponIssueEvent event = new CouponIssueEvent(couponId, userId, LocalDateTime.now());
        // join 성공은 브로커 접수 확인이다. Consumer 처리 완료나 최종 발급 성공을 기다리지 않는다.
        kafkaTemplate.send(TOPIC, String.valueOf(couponId), event).join();
    }
}
