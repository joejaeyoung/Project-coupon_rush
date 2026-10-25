package com.example.coupon;

import com.example.coupon.domain.Coupon;
import com.example.coupon.exception.CouponErrorCode;
import com.example.coupon.exception.CouponException;
import com.example.coupon.repository.CouponRepository;
import com.example.coupon.repository.IssuedCouponRepository;
import com.example.coupon.service.CouponKafkaProducer;
import com.example.coupon.service.CouponService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;

/** Kafka 발행이 실패하면 Redis 예약(재고·발급자)을 되돌리고 503을 낸다 */
@SpringBootTest
class CouponIssueCompensationTest {

    @Autowired
    private CouponService couponService;

    @Autowired
    private CouponRepository couponRepository;

    @Autowired
    private IssuedCouponRepository issuedCouponRepository;

    @MockitoBean
    private CouponKafkaProducer couponKafkaProducer;

    @Test
    @DisplayName("Kafka 발행에 실패하면 재고·발급자를 되돌리고 ISSUE_FAILED(503)를 낸다")
    void 발행_실패_시_예약_보상() {
        issuedCouponRepository.deleteAllInBatch();
        Long couponId = couponRepository.save(Coupon.builder()
                .totalQuantity(100)
                .startDate(LocalDateTime.now().minusDays(1))
                .endDate(LocalDateTime.now().plusDays(1))
                .build()).getId();
        couponService.initStock(couponId);
        doThrow(new IllegalStateException("Kafka 발행 실패")).when(couponKafkaProducer).publish(anyLong(), anyLong());

        assertThatThrownBy(() -> couponService.issueCoupon(couponId, 1L))
                .isInstanceOf(CouponException.class)
                .extracting("errorCode").isEqualTo(CouponErrorCode.ISSUE_FAILED);
        assertThat(couponService.getStock(couponId)).isEqualTo(100);   // 재고가 돌아왔다 (INCR)

        // 같은 사용자가 다시 발급받을 수 있어야 발급자 집합에서도 빠진 것이다 (SREM)
        doNothing().when(couponKafkaProducer).publish(anyLong(), anyLong());
        couponService.issueCoupon(couponId, 1L);
        assertThat(couponService.getStock(couponId)).isEqualTo(99);
        assertThat(issuedCouponRepository.count()).isZero();            // mock이라 Kafka·DB에는 아무것도 가지 않았다
    }
}
