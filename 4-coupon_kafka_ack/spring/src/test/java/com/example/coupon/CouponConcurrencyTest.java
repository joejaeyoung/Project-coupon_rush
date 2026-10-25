package com.example.coupon;

import com.example.coupon.domain.coupon.Coupon;
import com.example.coupon.domain.coupon.CouponStatus;
import com.example.coupon.domain.coupon.CouponType;
import com.example.coupon.exception.CouponException;
import com.example.coupon.repository.CouponRepository;
import com.example.coupon.repository.IssuedCouponRepository;
import com.example.coupon.service.CouponService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * 실제 MySQL·Redis·Kafka가 떠 있어야 한다 (docker compose up -d mysql redis kafka).
 * 발급 판정은 Redis에서 동기로 끝나지만 DB 저장은 Kafka Consumer가 하므로 await로 기다린다.
 */
@SpringBootTest
class CouponConcurrencyTest {

    @Autowired
    private CouponService couponService;

    @Autowired
    private CouponRepository couponRepository;

    @Autowired
    private IssuedCouponRepository issuedCouponRepository;

    private Long couponId;

    @BeforeEach
    void setUp() {
        issuedCouponRepository.deleteAll();
        couponId = saveCoupon(100);
        couponService.initStock(couponId);
    }

    @Test
    @DisplayName("1000명 동시 요청 시 정확히 100개만 발급된다")
    void 천명_동시_요청_100개_발급() throws InterruptedException {
        int totalUsers = 1000;
        ExecutorService executor = Executors.newFixedThreadPool(32);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(totalUsers);
        AtomicInteger successCount = new AtomicInteger();
        AtomicInteger failCount = new AtomicInteger();

        for (int i = 0; i < totalUsers; i++) {
            long userId = i + 1;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    couponService.issueCoupon(couponId, userId);
                    successCount.incrementAndGet();
                } catch (CouponException e) {
                    failCount.incrementAndGet(); // EXHAUSTED — 정상 탈락
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            });
        }
        startLatch.countDown();
        doneLatch.await();
        executor.shutdown();

        // Redis 판정은 동기 — 응답 시점에 확정
        assertThat(successCount.get()).isEqualTo(100);
        assertThat(successCount.get() + failCount.get()).isEqualTo(totalUsers);

        // DB 저장은 Kafka Consumer 비동기
        awaitIssuedCount(100);
        assertThat(couponRepository.findById(couponId).orElseThrow().getIssuedQuantity()).isEqualTo(100);
    }

    @Test
    @DisplayName("동일 사용자가 100번 동시 요청해도 1번만 발급된다")
    void 동일_사용자_중복_발급_방지() throws InterruptedException {
        int requestCount = 100;
        long sameUserId = 9999L;
        ExecutorService executor = Executors.newFixedThreadPool(32);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(requestCount);
        AtomicInteger successCount = new AtomicInteger();

        for (int i = 0; i < requestCount; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    couponService.issueCoupon(couponId, sameUserId);
                    successCount.incrementAndGet();
                } catch (CouponException e) {
                    // DUPLICATED — 기대한 동작
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            });
        }
        startLatch.countDown();
        doneLatch.await();
        executor.shutdown();

        assertThat(successCount.get()).isEqualTo(1);
        awaitIssuedCount(1);
    }

    private Long saveCoupon(int totalQuantity) {
        return couponRepository.save(Coupon.builder()
                .name("test")
                .description("선착순 테스트 쿠폰")
                .couponType(CouponType.FIXED_AMOUNT)
                .status(CouponStatus.ACTIVE)
                .discountValue(1000)
                .totalQuantity(totalQuantity)
                .validDays(30)
                .startDate(LocalDateTime.now().minusDays(1))
                .endDate(LocalDateTime.now().plusDays(1))
                .build()).getId();
    }

    private void awaitIssuedCount(long expected) {
        await().atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(200))
                .untilAsserted(() -> assertThat(issuedCouponRepository.count()).isEqualTo(expected));
    }
}
