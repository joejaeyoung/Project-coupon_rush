package com.example.coupon.service;

import com.example.coupon.domain.IssuedCoupon;
import com.example.coupon.event.CouponIssueEvent;
import com.example.coupon.repository.CouponRepository;
import com.example.coupon.repository.IssuedCouponRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * "coupon-issue" 이벤트를 받아 MySQL에 발급을 확정 저장한다.
 *
 * 재처리로 같은 이벤트가 두 번 올 수 있다(at-least-once). 먼저 발급 여부를 확인해 건너뛰고,
 * 그래도 겹치면 issued_coupon의 (coupon_id, user_id) 유니크 제약이 막는다.
 * 총량은 issued_quantity < total_quantity 조건부 UPDATE가 게이트다 — 발행 타임아웃 후 Redis를 되돌렸는데
 * 이벤트가 뒤늦게 도착해도 총량 초과는 막는다. 단, 재고가 남으면 503을 받은 사용자도 발급될 수 있다.
 * 적용할 수 없는 이벤트는 로그를 남기고 건너뛰며 Redis 예약을 보상하지 않는다.
 * enable-auto-commit=false 이므로 이 메서드가 예외로 끝나면 offset을 커밋하지 않고 컨테이너(DefaultErrorHandler)가
 * 실패 레코드를 기본 정책에 따라 다시 전달한다. 재시도 소진 후 별도 DLT 없이 건너뛸 수 있다.
 * DB 커밋과 offset 커밋은 별개이므로 그 사이 종료되면 이미 저장한 이벤트도 재전달될 수 있다.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class CouponKafkaConsumer {

    private final CouponRepository couponRepository;
    private final IssuedCouponRepository issuedCouponRepository;

    // UPDATE(총량 +1)와 INSERT를 한 트랜잭션으로 — INSERT가 유니크 제약에 걸리면 +1도 함께 되돌아간다
    @Transactional
    @KafkaListener(topics = CouponKafkaProducer.TOPIC)
    public void consume(CouponIssueEvent event) {
        if (issuedCouponRepository.existsByCouponIdAndUserId(event.couponId(), event.userId())) {
            log.info("이미 발급된 이벤트 건너뜀 — couponId={}, userId={}", event.couponId(), event.userId());
            return;
        }
        // 반환값은 남은 재고가 아닌 변경 행 수다. 0이면 INSERT 없이 정상 종료하여 소비를 진행한다.
        if (couponRepository.incrementIssuedQuantity(event.couponId()) == 0) {
            log.warn("DB에서 발급 거부(총량 초과 또는 쿠폰 없음) — couponId={}, userId={}", event.couponId(), event.userId());
            return;
        }
        // 프록시의 트랜잭션 커밋까지 끝난 뒤 컨테이너가 offset을 관리한다. 수동 acknowledge는 하지 않는다.
        issuedCouponRepository.save(new IssuedCoupon(event.couponId(), event.userId(), event.issuedAt()));
    }
}
