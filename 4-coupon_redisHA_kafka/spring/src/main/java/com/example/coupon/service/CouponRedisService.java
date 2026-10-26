package com.example.coupon.service;

import com.example.coupon.exception.CouponErrorCode;
import com.example.coupon.exception.CouponException;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;

/**
 * Redis 기반 재고·중복 제어. 판정은 여기서 끝나고 DB 저장은 Kafka 뒤로 넘어간다.
 *
 *   재고 카운터 : coupon:{couponId}:stock
 *   발급자 집합 : coupon:{couponId}:users
 *   발급 기간   : coupon:{couponId}:startDate / coupon:{couponId}:endDate
 *
 * Redis는 primary 1 · replica 1 · Sentinel 3 구성이고 복제는 비동기다. 승인한 예약이 replica에 닿기 전에
 * primary가 죽으면 failover 뒤 그 예약은 없다. 그래서 예약 직후 WAIT로 replica의 수신을 확인한 뒤에만 승인한다.
 */
@Component
public class CouponRedisService {

    private final StringRedisTemplate redisTemplate;
    private final int waitReplicas;
    private final long waitTimeoutMs;

    public CouponRedisService(StringRedisTemplate redisTemplate,
                              @Value("${coupon.redis.wait-replicas}") int waitReplicas,
                              @Value("${coupon.redis.wait-timeout-ms}") long waitTimeoutMs) {
        this.redisTemplate = redisTemplate;
        this.waitReplicas = waitReplicas;
        this.waitTimeoutMs = waitTimeoutMs;
    }

    private String stockKey(Long couponId)     { return "coupon:" + couponId + ":stock"; }
    private String usersKey(Long couponId)     { return "coupon:" + couponId + ":users"; }
    private String startDateKey(Long couponId) { return "coupon:" + couponId + ":startDate"; }
    private String endDateKey(Long couponId)   { return "coupon:" + couponId + ":endDate"; }

    // 실험 준비용: users 삭제는 reset-coupon.sh가 담당한다. 이 쓰기에는 WAIT를 적용하지 않는다.
    public void initStock(Long couponId, int totalQuantity, LocalDateTime startDate, LocalDateTime endDate) {
        redisTemplate.opsForValue().multiSet(Map.of(
                stockKey(couponId), String.valueOf(totalQuantity),
                startDateKey(couponId), startDate.toString(),
                endDateKey(couponId), endDate.toString()));
    }

    /**
     * 발급 가능 여부 선확인 — 기간과 재고를 MGET 한 번으로 읽는다.
     * 재고는 미리보기일 뿐이고 판정의 권위는 checkAndIssue의 SADD → DECR이다.
     * 소진 뒤 들어오는 요청(부하의 대부분)이 SADD·DECR·INCR·SREM 네 번 대신 한 번으로 끝난다.
     */
    public void validateIssuable(Long couponId) {
        List<String> values = redisTemplate.opsForValue().multiGet(
                List.of(startDateKey(couponId), endDateKey(couponId), stockKey(couponId)));
        if (values == null || values.contains(null)) {
            throw new CouponException(CouponErrorCode.NOT_FOUND); // initStock 미호출
        }
        LocalDateTime now = LocalDateTime.now();
        if (LocalDateTime.parse(values.get(0)).isAfter(now) || LocalDateTime.parse(values.get(1)).isBefore(now)) {
            throw new CouponException(CouponErrorCode.NOT_AVAILABLE);
        }
        if (Integer.parseInt(values.get(2)) <= 0) {
            throw new CouponException(CouponErrorCode.EXHAUSTED);
        }
    }

    public int getStock(Long couponId) {
        String stock = redisTemplate.opsForValue().get(stockKey(couponId));
        if (stock == null) {
            throw new CouponException(CouponErrorCode.NOT_FOUND);
        }
        return Integer.parseInt(stock);
    }

    /**
     * 중복 확인(SADD) → 재고 차감(DECR) → 복제 확인(WAIT).
     *
     * 명령 각각은 원자적이지만 둘 사이는 아니다. 재고가 음수가 되면 INCR·SREM으로 되돌린다.
     * SADD 뒤 DECR 전에 프로세스가 죽으면 그 사용자만 users에 남는다(재고는 그대로).
     * 이 창을 닫으려면 Lua나 outbox가 필요한데, 복잡도를 이유로 두지 않는다 (README "남은 한계").
     */
    public void checkAndIssue(Long couponId, Long userId) {
        Long added = redisTemplate.opsForSet().add(usersKey(couponId), String.valueOf(userId));
        if (added == null || added == 0) {
            throw new CouponException(CouponErrorCode.DUPLICATED);
        }
        // SADD 성공자만 차감한다. 선확인 이후 경합으로 재고가 음수가 되면 이 요청 몫을 되돌린다.
        Long remaining = redisTemplate.opsForValue().decrement(stockKey(couponId));
        if (remaining == null || remaining < 0) {
            rollback(couponId, userId);
            throw new CouponException(CouponErrorCode.EXHAUSTED);
        }
        // false(복제 수 부족)일 때만 여기서 보상한다. WAIT가 예외를 던지면 자동 보상되지 않는다.
        if (!replicated()) {
            rollback(couponId, userId);
            throw new CouponException(CouponErrorCode.REPLICATION_TIMEOUT);
        }
    }

    /**
     * 이 요청의 재고 1개와 사용자 예약을 되돌린다. 소진 경합·복제 미확인·발행 실패에서 호출한다.
     * 두 명령은 원자적이지 않고 반복 호출에 멱등하지도 않다(INCR가 반복됨).
     * 보상 쓰기에는 WAIT가 없으며 중간 실패 시 나머지 명령을 재시도하는 작업도 없다.
     */
    public void rollback(Long couponId, Long userId) {
        redisTemplate.opsForValue().increment(stockKey(couponId));
        redisTemplate.opsForSet().remove(usersKey(couponId), String.valueOf(userId));
    }

    /**
     * 이 커넥션의 앞선 쓰기(SADD·DECR)가 replica에 닿을 때까지 기다린다 — Redis WAIT.
     * Spring Data에는 WAIT가 없어 Lettuce 커맨드를 직접 쓴다. WAIT는 응답이 올 때까지 공유 커넥션을 막으므로
     * replica가 느리거나 죽어 있으면 모든 요청이 timeout만큼 직렬화된다 — 그게 "복제될 때까지 primary를 세우는" 비용이다.
     * 현재 기본 공유 native connection을 전제로 한다. 풀로 변경할 때는 SADD·DECR·WAIT를
     * 같은 연결에 묶어야 한다. WAIT만 다른 연결로 보내면 원래 예약 쓰기의 복제를 확인하지 못한다.
     * 복제 수신 확인은 디스크 fsync나 모든 failover에서의 무손실 보장이 아니다.
     */
    @SuppressWarnings("unchecked")
    private boolean replicated() {
        if (waitReplicas == 0) {
            return true;
        }
        try {
            Long acked = redisTemplate.execute((RedisCallback<Long>) connection ->
                    ((RedisClusterAsyncCommands<byte[], byte[]>) connection.getNativeConnection())
                            .waitForReplication(waitReplicas, waitTimeoutMs).toCompletableFuture().join());
            return acked != null && acked >= waitReplicas;
        } catch (CompletionException e) {
            throw new CouponException(CouponErrorCode.STORE_UNAVAILABLE, e); // WAIT 도중 커넥션이 끊김 (failover 중)
        }
    }
}
