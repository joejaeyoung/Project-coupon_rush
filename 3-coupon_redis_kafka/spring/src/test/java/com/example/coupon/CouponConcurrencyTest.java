package com.example.coupon;

import com.example.coupon.domain.coupon.Coupon;
import com.example.coupon.domain.coupon.Status;
import com.example.coupon.repository.CouponRepository;
import com.example.coupon.repository.IssuedCouponRepository;
import com.example.coupon.exception.CouponException;
import com.example.coupon.service.CouponRedisService;
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

@SpringBootTest
class CouponConcurrencyTest {

    @Autowired
    private CouponService couponService;

    @Autowired
    private IssuedCouponRepository issuedCouponRepository;

    @Autowired
    private CouponRepository couponRepository;

    @Autowired
    private CouponRedisService couponRedisService;

    private Long couponId;

    @BeforeEach
    void setUp() {
        issuedCouponRepository.deleteAll();
        Coupon coupon = new Coupon();
        coupon.setName("test");
        coupon.setDescription("test_coupon");
        coupon.setStartDate(LocalDateTime.now().minusDays(1));
        coupon.setEndDate(LocalDateTime.now().plusDays(1));
        coupon.setStatus(Status.ACTIVE);
        coupon.setTotalQuantity(100);
        coupon.setIssuedQuantity(0);
        coupon.setValidDays(30);

        couponId = couponRepository.save(coupon).getId();

        // Phase 2와 동일하게 재고·기간을 Redis에 올린다.
        // Phase 3에서도 발급 판정은 여전히 Redis가 한다. 달라지는 건 그 뒤 DB 쓰기 경로뿐이다.
        couponRedisService.initStock(
                couponId,
                coupon.getTotalQuantity(),
                coupon.getStartDate(),
                coupon.getEndDate());
    }

    // ─────────────────────────────────────────────────────────
    // 시나리오 1: 1000명 동시 요청 → 정확히 100개만 발급
    //
    // CountDownLatch(1) startLatch  : 모든 스레드가 동시에 출발하도록 신호 대기
    // CountDownLatch(N) doneLatch   : 모든 스레드 완료 대기
    // AtomicInteger successCount    : 스레드 안전한 카운터
    // ─────────────────────────────────────────────────────────
    @Test
    @DisplayName("1000명 동시 요청 시 정확히 100개만 발급된다")
    void 천명_동시_요청_100개_발급() throws InterruptedException {
        int totalUsers   = 1000;
        int threadCount  = 32;   // 스레드 풀 크기 (동시 실행 스레드 수)

        ExecutorService executor     = Executors.newFixedThreadPool(threadCount);
        CountDownLatch   startLatch  = new CountDownLatch(1);     // 동시 출발 신호
        CountDownLatch   doneLatch   = new CountDownLatch(totalUsers); // 완료 대기
        AtomicInteger    successCount = new AtomicInteger(0);
        AtomicInteger    failCount    = new AtomicInteger(0);

        for (int i = 0; i < totalUsers; i++) {
            long userId = i + 1;
            executor.submit(() -> {
                try {
                    startLatch.await(); // 신호 올 때까지 대기

                    couponService.issueCoupon(couponId, userId);
                    successCount.incrementAndGet();

                } catch (CouponException e) {
                    // 재고 소진(EXHAUSTED) · 중복 발급(DUPLICATED) — 정상적인 탈락
                    failCount.incrementAndGet();
                } catch (InterruptedException e) {
                    failCount.incrementAndGet();
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown(); // 완료 신호
                }
            });
        }

        startLatch.countDown(); // 모든 스레드 동시 출발
        doneLatch.await();      // 전부 끝날 때까지 대기
        executor.shutdown();

        System.out.println("성공: " + successCount.get());
        System.out.println("실패: " + failCount.get());

        // Redis 판정 결과는 동기 — 응답 시점에 이미 확정되어 있다
        assertThat(successCount.get()).isEqualTo(100);
        assertThat(successCount.get() + failCount.get()).isEqualTo(totalUsers);

        // DB 쓰기는 Kafka Consumer가 비동기로 처리한다.
        // 발급 API가 202를 돌려준 시점에는 아직 INSERT 전일 수 있으므로 즉시 단언하면 실패한다.
        // Redis 판정을 통과한 100건을 Consumer가 저장할 때까지 기다린다.
        await().atMost(Duration.ofSeconds(30))
               .pollInterval(Duration.ofMillis(200))
               .untilAsserted(() ->
                       assertThat(issuedCouponRepository.count()).isEqualTo(100));
    }

    // ─────────────────────────────────────────────────────────
    // 시나리오 2: 동일 사용자가 100번 동시 요청 → 정확히 1번만 성공
    // ─────────────────────────────────────────────────────────
    @Test
    @DisplayName("동일 사용자가 100번 동시 요청해도 1번만 발급된다")
    void 동일_사용자_중복_발급_방지() throws InterruptedException {
        int requestCount = 100;
        long sameUserId  = 9999L;

        ExecutorService executor     = Executors.newFixedThreadPool(32);
        CountDownLatch   startLatch  = new CountDownLatch(1);
        CountDownLatch   doneLatch   = new CountDownLatch(requestCount);
        AtomicInteger    successCount = new AtomicInteger(0);

        for (int i = 0; i < requestCount; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await();

                    couponService.issueCoupon(couponId, sameUserId);
                    successCount.incrementAndGet();
                } catch (CouponException e) {
                    // 두 번째 이후 요청은 DUPLICATED — 기대한 동작이므로 무시
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

        System.out.println("성공 횟수: " + successCount.get());

        assertThat(successCount.get()).isEqualTo(1);

        // Redis에서 중복 요청을 차단한 결과다. Kafka 직접 재전달은 experiments/kafka에서 검증한다.
        await().atMost(Duration.ofSeconds(30))
               .pollInterval(Duration.ofMillis(200))
               .untilAsserted(() ->
                       assertThat(issuedCouponRepository.count()).isEqualTo(1));
    }
}
