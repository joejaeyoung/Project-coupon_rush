package com.example.coupon.service;

import com.example.coupon.event.CouponIssueEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

/**
 * Kafka Producer — 모든 쿠폰의 발급 이벤트를 하나의 "coupon-issue" 토픽에 발행한다.
 * 토픽은 메시지 종류별 이름이고, 파티션은 토픽 안에서 이벤트가 순서대로 쌓이는 로그다.
 * 쿠폰마다 별도 토픽이나 파티션을 만들지 않는다. 이 모듈은 파티션 수를 지정하지 않으므로
 * Kafka 기본 설정으로 새 토픽이 자동 생성되면 파티션은 1개(partition 0)다.
 * key=String(couponId)는 파티션 선택 기준이지 파티션 생성 명령이 아니다. 파티션이 여러 개라면
 * 같은 쿠폰 ID는 같은 파티션에 가지만, 다른 쿠폰 ID도 같은 파티션에 갈 수 있다.
 *
 * 브로커 ACK(acks=all)를 받을 때까지 기다린다. 포기 시점은 producer 설정이 정한다 —
 * send() 자체가 max.block.ms 안에 메타데이터·버퍼를 못 얻으면 동기 예외(TimeoutException), 그 뒤 delivery.timeout.ms 안에 ACK가 없으면
 * join()이 CompletionException을 던진다. 둘 다 RuntimeException이라 CouponService가 한 catch로 받아 예약을 되돌린다.
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
