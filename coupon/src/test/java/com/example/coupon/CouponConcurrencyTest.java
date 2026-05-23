package com.example.coupon;

import com.example.coupon.repository.IssuedCouponRepository;
import com.example.coupon.service.CouponService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
class CouponConcurrencyTest {

    @Autowired
    private CouponService couponService;

    @Autowired
    private IssuedCouponRepository issuedCouponRepository;

    // ─────────────────────────────────────────────────────────
    // TODO: @BeforeEach에서 테스트용 쿠폰 데이터를 DB에 미리 넣어두세요
    //   - totalQuantity: 100
    //   - status: ACTIVE
    //   - startDate: 과거 시간
    //   - endDate: 미래 시간
    //   테스트마다 데이터가 초기화되어야 하므로 @BeforeEach에서 저장합니다
    // ─────────────────────────────────────────────────────────
    private Long couponId;

    @BeforeEach
    void setUp() {
        issuedCouponRepository.deleteAll();
        // TODO: 테스트용 Coupon 생성 후 저장, couponId 저장
        //   Coupon coupon = new Coupon(...);
        //   couponId = couponRepository.save(coupon).getId();
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

                    // TODO: couponService.issueCoupon(couponId, userId) 호출
                    //       성공 시 successCount.incrementAndGet()
                    //       CouponException 발생 시 failCount.incrementAndGet()

                } catch (InterruptedException e) {
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

        // TODO: assertThat(successCount.get()).isEqualTo(100);
        // TODO: assertThat(issuedCouponRepository.count()).isEqualTo(100);
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

                    // TODO: couponService.issueCoupon(couponId, sameUserId) 호출
                    //       성공 시 successCount.incrementAndGet()
                    //       CouponException은 무시 (중복/소진 예외)

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

        // TODO: assertThat(successCount.get()).isEqualTo(1);
    }
}
