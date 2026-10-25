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
 * 이벤트가 뒤늦게 도착한 경우가 여기서 걸린다. 적용할 수 없는 이벤트는 로그를 남기고 건너뛴다.
 * enable-auto-commit=false 이므로 이 메서드가 예외로 끝나면 offset을 커밋하지 않고 컨테이너(DefaultErrorHandler)가
 * 같은 레코드를 다시 넣는다 — 기본 10회, 그 뒤엔 로그만 남기고 건너뛴다(DLT 없음, README "남은 한계").
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
        if (couponRepository.incrementIssuedQuantity(event.couponId()) == 0) {
            log.warn("DB에서 발급 거부(총량 초과 또는 쿠폰 없음) — couponId={}, userId={}", event.couponId(), event.userId());
            return;
        }
        issuedCouponRepository.save(new IssuedCoupon(event.couponId(), event.userId(), event.issuedAt()));
    }
}
