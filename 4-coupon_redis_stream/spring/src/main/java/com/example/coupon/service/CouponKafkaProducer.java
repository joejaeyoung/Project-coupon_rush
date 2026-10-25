package com.example.coupon.service;

import com.example.coupon.event.CouponIssueEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Kafka Producer — 발급 이벤트를 "coupon-issue" 토픽에 발행한다.
 *
 * 브로커 ACK(acks=all)를 받을 때까지 기다린다. CouponStreamRelay는 이 메서드가 정상 반환한 뒤에만 Stream 이벤트를 XACK 한다.
 * couponId를 Key로 쓰므로 같은 쿠폰의 이벤트는 같은 파티션에 발급 순서대로 쌓인다.
 */
@Component
@RequiredArgsConstructor
public class CouponKafkaProducer {

    static final String TOPIC = "coupon-issue";
    private static final Duration ACK_TIMEOUT = Duration.ofSeconds(10);

    private final KafkaTemplate<String, CouponIssueEvent> kafkaTemplate;

    public void publish(CouponIssueEvent event) {
        try {
            kafkaTemplate.send(TOPIC, String.valueOf(event.getCouponId()), event)
                    .get(ACK_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Kafka 발행이 중단되었습니다", e);
        } catch (ExecutionException | TimeoutException e) {
            throw new IllegalStateException(
                    "Kafka 발행 실패 — couponId=" + event.getCouponId() + ", userId=" + event.getUserId(), e);
        }
    }
}
