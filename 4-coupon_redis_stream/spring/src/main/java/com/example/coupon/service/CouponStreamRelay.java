package com.example.coupon.service;

import com.example.coupon.event.CouponIssueEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Redis Stream → Kafka relay.
 *
 * 매 틱마다 쿠폰별 이벤트 Stream을 consumer group으로 읽어 Kafka에 발행하고, 브로커 ACK를 받은 뒤에야 XACK 한다.
 * 발행 도중 죽으면 이벤트는 PEL(pending entries list)에 남고, 재시작한 relay가 같은 consumer 이름으로 PEL부터 다시 읽는다.
 * 따라서 같은 이벤트가 Kafka에 두 번 갈 수는 있고(at-least-once), 그 중복은 CouponKafkaConsumer가 걸러낸다.
 *
 * Kafka가 내려가 있는 동안에도 API는 계속 예약(202)을 받는다 — 이벤트는 Redis에 쌓였다가 Kafka 복구 후 흘러간다.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class CouponStreamRelay {

    // ponytail: relay 인스턴스 하나를 전제로 한 고정 consumer 이름. 다중 인스턴스로 가면
    //           인스턴스별 이름 + XAUTOCLAIM으로 죽은 consumer의 PEL을 넘겨받는 로직이 필요하다.
    public static final String CONSUMER_NAME = "coupon-relay";
    private static final int BATCH_SIZE = 100;

    private final CouponRedisService couponRedisService;
    private final CouponKafkaProducer couponKafkaProducer;

    @Scheduled(fixedDelay = 100)
    public void relay() {
        for (Long couponId : couponRedisService.getStreamCouponIds()) {
            try {
                relayCoupon(couponId);
            } catch (RuntimeException e) {
                // XACK 되지 않은 이벤트는 다음 틱에 PEL부터 다시 읽으므로 여기서는 기록만 한다
                log.warn("쿠폰 {} 이벤트 relay 실패 — 다음 틱에 재시도: {}", couponId, e.getMessage());
            }
        }
    }

    private void relayCoupon(Long couponId) {
        for (MapRecord<String, Object, Object> record : couponRedisService.readEvents(couponId, CONSUMER_NAME, BATCH_SIZE)) {
            couponKafkaProducer.publish(CouponIssueEvent.from(record.getValue()));
            couponRedisService.acknowledge(couponId, record.getId());
        }
    }
}
