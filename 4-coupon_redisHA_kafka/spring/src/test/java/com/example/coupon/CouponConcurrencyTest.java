package com.example.coupon;

import com.example.coupon.domain.Coupon;
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
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.IntToLongFunction;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * 실제 MySQL·Redis·Kafka가 떠 있어야 한다 (docker compose up -d --wait).
 * 발급 판정은 Redis에서 동기로 끝나지만 DB 저장은 Kafka Consumer가 하므로 await로 기다린다.
 * 테스트 클래스마다 별도 컨텍스트(Consumer 포함)가 떠서 같은 그룹에 Consumer가 여러 개일 수 있다 — 단일 파티션이라 결과는 같다.
 * count()는 전체 행이므로 setUp의 deleteAllInBatch에 의존한다.
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
        issuedCouponRepository.deleteAllInBatch();
        couponId = couponRepository.save(Coupon.builder()
                .totalQuantity(100)
                .startDate(LocalDateTime.now().minusDays(1))
                .endDate(LocalDateTime.now().plusDays(1))
                .build()).getId();
        couponService.initStock(couponId);
    }

    @Test
    @DisplayName("1000명 동시 요청 시 정확히 100개만 발급된다")
    void 천명_동시_요청_100개_발급() throws Exception {
        assertThat(issueConcurrently(1000, i -> i + 1)).isEqualTo(100);  // Redis 판정은 동기 — 응답 시점에 확정
        awaitIssuedCount(100);                                            // DB 저장은 Kafka Consumer 비동기
        assertThat(couponRepository.findById(couponId).orElseThrow().getIssuedQuantity()).isEqualTo(100);
    }

    @Test
    @DisplayName("동일 사용자가 100번 동시 요청해도 1번만 발급된다")
    void 동일_사용자_중복_발급_방지() throws Exception {
        assertThat(issueConcurrently(100, i -> 9999L)).isEqualTo(1);
        awaitIssuedCount(1);
    }

    /** n개 요청을 32스레드로 동시에 보내고 CouponException 없이 끝난 요청 수를 돌려준다. 그 외 예외는 테스트 실패 */
    private long issueConcurrently(int requests, IntToLongFunction userIdOf) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(32);
        try {
            List<Callable<Boolean>> tasks = IntStream.range(0, requests).<Callable<Boolean>>mapToObj(i -> () -> {
                try {
                    couponService.issueCoupon(couponId, userIdOf.applyAsLong(i));
                    return true;
                } catch (CouponException e) {
                    return false; // EXHAUSTED · DUPLICATED — 정상 탈락
                }
            }).toList();
            long success = 0;
            for (Future<Boolean> result : pool.invokeAll(tasks)) {
                if (result.get()) {
                    success++;
                }
            }
            return success;
        } finally {
            pool.shutdown();
        }
    }

    private void awaitIssuedCount(long expected) {
        await().atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(200))
                .untilAsserted(() -> assertThat(issuedCouponRepository.count()).isEqualTo(expected));
    }
}
