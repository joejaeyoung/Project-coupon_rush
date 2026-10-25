package com.example.coupon;

import com.example.coupon.domain.coupon.Coupon;
import com.example.coupon.domain.coupon.CouponStatus;
import com.example.coupon.domain.coupon.CouponType;
import com.example.coupon.exception.CouponException;
import com.example.coupon.repository.CouponRepository;
import com.example.coupon.repository.IssuedCouponRepository;
import com.example.coupon.service.CouponRedisService;
import com.example.coupon.service.CouponService;
import com.example.coupon.service.CouponStreamRelay;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.StreamReadOptions;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * 실제 MySQL·Redis·Kafka가 떠 있어야 한다 (docker compose up -d mysql redis kafka).
 * 발급 판정은 Redis에서 동기로 끝나지만 DB 저장은 relay → Kafka → Consumer를 거치므로 await로 기다린다.
 */
@SpringBootTest
class CouponConcurrencyTest {

    @Autowired
    private CouponService couponService;

    @Autowired
    private CouponRepository couponRepository;

    @Autowired
    private IssuedCouponRepository issuedCouponRepository;

    @Autowired
    private StringRedisTemplate redisTemplate;

    private Long couponId;

    @BeforeEach
    void setUp() {
        issuedCouponRepository.deleteAll();
        couponId = saveCoupon(100);
        couponService.initStock(couponId); // 재고·기간·이벤트 Stream·consumer group
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

        // DB 저장은 relay → Kafka → Consumer 비동기
        awaitIssuedCount(100);

        // 예약 100건이 모두 Stream에 기록됐고, 전부 Kafka ACK 후 XACK 됐다
        assertThat(redisTemplate.opsForStream().size(eventsKey(couponId))).isEqualTo(100);
        assertThat(pendingCount(couponId)).isZero();
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
        assertThat(redisTemplate.opsForStream().size(eventsKey(couponId))).isEqualTo(1);
        assertThat(pendingCount(couponId)).isZero();
    }

    @Test
    @DisplayName("Kafka 발행 전에 멈춘 이벤트(PEL)는 relay가 이어서 발급한다")
    @SuppressWarnings("unchecked")
    void 미확인_이벤트_복구() {
        // relay가 XREADGROUP으로 이벤트를 받은 직후, Kafka 발행 전에 죽은 상황을 재현한다.
        // relay 순회 목록에 아직 없는 쿠폰에 이벤트를 넣고, relay와 같은 consumer 이름으로 읽기만 하고 XACK 하지 않는다.
        Long recoveringCouponId = saveCoupon(10);
        String key = eventsKey(recoveringCouponId);
        redisTemplate.opsForStream().createGroup(key, ReadOffset.from("0"), CouponRedisService.STREAM_GROUP);
        redisTemplate.opsForStream().add(key, Map.of(
                "couponId", recoveringCouponId.toString(), "userId", "42", "issuedAt", LocalDateTime.now().toString()));
        redisTemplate.opsForStream().read(
                Consumer.from(CouponRedisService.STREAM_GROUP, CouponStreamRelay.CONSUMER_NAME),
                StreamReadOptions.empty().count(1),
                StreamOffset.create(key, ReadOffset.lastConsumed()));
        assertThat(pendingCount(recoveringCouponId)).isEqualTo(1);

        // "재시작" — 쿠폰이 relay 순회 목록에 들어가면 다음 틱에 PEL부터 읽는다
        couponService.initStock(recoveringCouponId);

        awaitIssuedCount(1);
        assertThat(issuedCouponRepository.existsByCouponIdAndUserId(recoveringCouponId, 42L)).isTrue();
        assertThat(pendingCount(recoveringCouponId)).isZero();
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

    /** CouponRedisService의 key 규칙과 동일 */
    private String eventsKey(Long id) {
        return "coupon:{" + id + "}:events";
    }

    private long pendingCount(Long id) {
        return redisTemplate.opsForStream().pending(eventsKey(id), CouponRedisService.STREAM_GROUP).getTotalPendingMessages();
    }

    private void awaitIssuedCount(long expected) {
        await().atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(200))
                .untilAsserted(() -> assertThat(issuedCouponRepository.count()).isEqualTo(expected));
    }
}
