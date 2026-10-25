package com.example.coupon;

import com.example.coupon.domain.coupon.Coupon;
import com.example.coupon.domain.coupon.CouponStatus;
import com.example.coupon.domain.coupon.CouponType;
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
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyLong;
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

    @Autowired
    private StringRedisTemplate redisTemplate;

    @MockitoBean
    private CouponKafkaProducer couponKafkaProducer;

    @Test
    @DisplayName("Kafka 발행에 실패하면 재고·발급자를 되돌리고 ISSUE_FAILED(503)를 낸다")
    void 발행_실패_시_예약_보상() {
        issuedCouponRepository.deleteAll();
        Long couponId = couponRepository.save(Coupon.builder()
                .name("test").description("보상 테스트 쿠폰")
                .couponType(CouponType.FIXED_AMOUNT).status(CouponStatus.ACTIVE)
                .discountValue(1000).totalQuantity(100).validDays(30)
                .startDate(LocalDateTime.now().minusDays(1)).endDate(LocalDateTime.now().plusDays(1))
                .build()).getId();
        couponService.initStock(couponId);
        doThrow(new IllegalStateException("Kafka 발행 실패")).when(couponKafkaProducer).publish(anyLong(), anyLong());

        assertThatThrownBy(() -> couponService.issueCoupon(couponId, 1L))
                .isInstanceOf(CouponException.class)
                .extracting("errorCode").isEqualTo(CouponErrorCode.ISSUE_FAILED);

        assertThat(couponService.getStock(couponId)).isEqualTo(100);
        assertThat(redisTemplate.opsForSet().isMember("coupon:" + couponId + ":users", "1")).isFalse();
        assertThat(issuedCouponRepository.count()).isZero();
    }
}
