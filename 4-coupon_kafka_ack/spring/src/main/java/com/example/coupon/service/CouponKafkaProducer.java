package com.example.coupon.service;

import com.example.coupon.event.CouponIssueEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Kafka Producer — 발급 이벤트를 "coupon-issue" 토픽에 발행한다.
 *
 * 브로커 ACK(acks=all)를 받을 때까지 기다린다. 그래야 호출 측이 "Kafka에 갔는지"를 알고 실패 시 Redis를 되돌릴 수 있다.
 * 대기 시간은 producer의 delivery.timeout.ms(10s)보다 길게 잡아, 반환 시점에는 producer가 재시도를 끝낸 상태다.
 * couponId를 Key로 쓰므로 같은 쿠폰의 이벤트는 같은 파티션에 발급 순서대로 쌓인다.
 */
@Component
@RequiredArgsConstructor
public class CouponKafkaProducer {

    static final String TOPIC = "coupon-issue";
    private static final Duration ACK_TIMEOUT = Duration.ofSeconds(12);

    private final KafkaTemplate<String, CouponIssueEvent> kafkaTemplate;

    public void publish(Long couponId, Long userId) {
        CouponIssueEvent event = new CouponIssueEvent(couponId, userId, LocalDateTime.now().toString());
        try {
            kafkaTemplate.send(TOPIC, String.valueOf(couponId), event)
                    .get(ACK_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Kafka 발행이 중단되었습니다", e);
        } catch (ExecutionException | TimeoutException e) {
            throw new IllegalStateException("Kafka 발행 실패 — couponId=" + couponId + ", userId=" + userId, e);
        }
    }
}
