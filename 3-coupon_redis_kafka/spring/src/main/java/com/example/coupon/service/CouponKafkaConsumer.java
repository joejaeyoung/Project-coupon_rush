package com.example.coupon.service;

import com.example.coupon.domain.coupon.Coupon;
import com.example.coupon.domain.issuedCoupon.IssuedCoupon;
import com.example.coupon.domain.issuedCoupon.Status;
import com.example.coupon.event.CouponIssueEvent;
import com.example.coupon.repository.CouponRepository;
import com.example.coupon.repository.IssuedCouponRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;

/**
 * Kafka Consumer — "coupon-issue" Topic에서 이벤트를 꺼내 MySQL에 저장한다.
 *
 * [흐름]
 *   CouponKafkaProducer.publish() → Kafka → 이 메서드 → MySQL INSERT
 *
 * [왜 @Transactional이 여기에 붙는가]
 *   Phase 2에서는 CouponService.issueCoupon() 안의 TransactionTemplate이 DB 쓰기를 담당했다.
 *   Phase 3에서는 DB 쓰기가 Consumer로 이동했으므로 @Transactional도 여기에 붙는다.
 *   CouponService.issueCoupon()에는 이제 트랜잭션이 없다 (Kafka 발행만 하면 되니까).
 *
 * [enable-auto-commit: false 의 의미]
 *   Kafka는 Consumer가 메시지를 처리했다는 신호로 "Offset 커밋"을 보낸다.
 *   false로 설정하면 Spring 컨테이너가 AckMode에 따라 커밋을 관리한다.
 *   BATCH 모드는 poll에서 받은 레코드를 처리한 뒤 커밋한다.
 *   DB 커밋 후 오프셋 커밋 전에 종료되면 이미 저장한 이벤트도 재전달될 수 있다.
 *
 * [멱등성(Idempotency)이란]
 *   Kafka는 At-least-once 전달을 보장한다.
 *   즉, 네트워크 장애 등으로 같은 메시지가 두 번 올 수 있다.
 *   이때 DB INSERT를 두 번 하면 쿠폰이 중복 발급된다.
 *   → "이미 처리한 이벤트인지 먼저 확인하고, 있으면 그냥 넘어간다"
 *   → 이처럼 같은 연산을 여러 번 해도 결과가 달라지지 않는 성질을 멱등성이라 한다.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class CouponKafkaConsumer {

    private final CouponRepository couponRepository;
    private final IssuedCouponRepository issuedCouponRepository;

    /**
     * "coupon-issue" Topic 이벤트 소비 → MySQL issued_coupon 테이블에 INSERT
     */
    @Transactional
    @KafkaListener(id = "coupon-issuer", topics = "${coupon.kafka.topic:coupon-issue}",
            groupId = "${spring.kafka.consumer.group-id:coupon-group}",
            concurrency = "${coupon.kafka.concurrency:1}")
    public void consume(CouponIssueEvent event) {

        // 멱등성 체크
        if (issuedCouponRepository.existsByCoupon_IdAndUserId(event.getCouponId(), event.getUserId())) {
            log.warn("{} 쿠폰 : {} 유저가 중복 시도", event.getCouponId(), event.getUserId());
            return ;
        }

        // 쿠폰 중복 조회 - 중복 시 런타임 에러로 재처리를 위해 Kafka가 재전송하게 됨
        Coupon coupon = couponRepository.findById(event.getCouponId())
                .orElseThrow(() -> new RuntimeException("Coupon not found: " + event.getCouponId()));

        // DB 에서 + 1
        couponRepository.incrementIssuedQuantity(event.getCouponId());


        // IssuedCoupon 생성 및 저장
        IssuedCoupon issuedCoupon = new IssuedCoupon();
        issuedCoupon.setCoupon(coupon);
        issuedCoupon.setUserId(event.getUserId());
        issuedCoupon.setStatus(Status.ISSUED);
        issuedCoupon.setIssuedAt(LocalDateTime.parse(event.getIssuedAt()));
        issuedCoupon.setExpiredAt(issuedCoupon.getIssuedAt().plusDays(coupon.getValidDays()));
        issuedCouponRepository.save(issuedCoupon);
    }
}
