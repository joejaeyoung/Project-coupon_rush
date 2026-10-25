package com.example.coupon.service;

import com.example.coupon.domain.coupon.Coupon;
import com.example.coupon.domain.issuedcoupon.IssuedCoupon;
import com.example.coupon.event.CouponIssueEvent;
import com.example.coupon.exception.CouponErrorCode;
import com.example.coupon.exception.CouponException;
import com.example.coupon.repository.CouponRepository;
import com.example.coupon.repository.IssuedCouponRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;

/**
 * Kafka Consumer — "coupon-issue" 이벤트를 받아 MySQL에 발급을 확정 저장한다.
 *
 * relay의 재발행과 Kafka 재전송 때문에 같은 이벤트가 두 번 올 수 있다(at-least-once).
 * 먼저 발급 여부를 확인해 건너뛰고, 그래도 겹치면 issued_coupon의 (coupon_id, user_id) 유니크 제약이 막는다.
 * enable-auto-commit=false 이므로 이 메서드가 예외로 끝나면 Offset이 커밋되지 않아 Kafka가 재전송한다.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class CouponKafkaConsumer {

    private final CouponRepository couponRepository;
    private final IssuedCouponRepository issuedCouponRepository;

    @Transactional
    @KafkaListener(topics = CouponKafkaProducer.TOPIC)
    public void consume(CouponIssueEvent event) {
        if (issuedCouponRepository.existsByCouponIdAndUserId(event.getCouponId(), event.getUserId())) {
            log.info("이미 발급된 이벤트 건너뜀 — couponId={}, userId={}", event.getCouponId(), event.getUserId());
            return;
        }

        Coupon coupon = couponRepository.findById(event.getCouponId())
                .orElseThrow(() -> new CouponException(CouponErrorCode.NOT_FOUND));

        couponRepository.incrementIssuedQuantity(coupon.getId());
        issuedCouponRepository.save(IssuedCoupon.issue(coupon, event.getUserId(), LocalDateTime.parse(event.getIssuedAt())));
    }
}
