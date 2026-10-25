package com.example.coupon.service;

import com.example.coupon.exception.CouponErrorCode;
import com.example.coupon.exception.CouponException;
import lombok.RequiredArgsConstructor;
import org.springframework.core.NestedExceptionUtils;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.StreamReadOptions;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Redis 기반 재고·중복 제어 + 발급 이벤트 Stream.
 *
 * Key 규칙 — 쿠폰 ID를 해시 태그 {}로 감싸 Redis Cluster에서도 한 쿠폰의 키가 같은 슬롯에 놓이게 한다.
 * 그래야 Lua 스크립트가 재고·발급자·Stream 세 키를 한 번에 다룰 수 있다.
 *
 *   재고 카운터 : coupon:{couponId}:stock
 *   발급자 집합 : coupon:{couponId}:users
 *   발급 이벤트 : coupon:{couponId}:events      (Stream, consumer group "coupon-relay")
 *   발급 기간   : coupon:{couponId}:startDate / coupon:{couponId}:endDate
 *   Stream 목록 : coupon:streams                (relay가 순회할 쿠폰 ID 집합)
 */
@Service
@RequiredArgsConstructor
public class CouponRedisService {

    public static final String STREAM_GROUP = "coupon-relay";
    private static final String STREAM_INDEX_KEY = "coupon:streams";

    // issue-coupon.lua 반환 코드
    private static final long RESULT_DUPLICATED = -1;
    private static final long RESULT_EXHAUSTED = -2;
    private static final long RESULT_NOT_INITIALIZED = -3;

    private final StringRedisTemplate redisTemplate;
    private final RedisScript<Long> issueCouponScript;

    private String stockKey(Long couponId)     { return "coupon:{" + couponId + "}:stock"; }
    private String usersKey(Long couponId)     { return "coupon:{" + couponId + "}:users"; }
    private String eventsKey(Long couponId)    { return "coupon:{" + couponId + "}:events"; }
    private String startDateKey(Long couponId) { return "coupon:{" + couponId + "}:startDate"; }
    private String endDateKey(Long couponId)   { return "coupon:{" + couponId + "}:endDate"; }

    /** 재고·기간을 올리고, 발급 이벤트 Stream과 consumer group을 만든 뒤 relay 순회 목록에 등록한다 */
    public void initStock(Long couponId, int totalQuantity, LocalDateTime startDate, LocalDateTime endDate) {
        redisTemplate.opsForValue().set(stockKey(couponId), String.valueOf(totalQuantity));
        redisTemplate.opsForValue().set(startDateKey(couponId), startDate.toString());
        redisTemplate.opsForValue().set(endDateKey(couponId), endDate.toString());
        createStreamGroup(couponId);
        redisTemplate.opsForSet().add(STREAM_INDEX_KEY, String.valueOf(couponId));
    }

    private void createStreamGroup(Long couponId) {
        try {
            // Stream이 없으면 빈 Stream을 함께 만든다(MKSTREAM). "0"이므로 기존 이벤트가 있어도 처음부터 전달한다.
            redisTemplate.opsForStream().createGroup(eventsKey(couponId), ReadOffset.from("0"), STREAM_GROUP);
        } catch (RedisSystemException e) {
            // 이미 그룹이 있으면 BUSYGROUP — 재초기화는 정상 경로이므로 무시한다
            String cause = String.valueOf(NestedExceptionUtils.getMostSpecificCause(e).getMessage());
            if (!cause.contains("BUSYGROUP")) {
                throw e;
            }
        }
    }

    /** 발급 기간 검증. 키가 없으면 initStock 미호출이므로 NOT_FOUND */
    public void validatePeriod(Long couponId) {
        String start = redisTemplate.opsForValue().get(startDateKey(couponId));
        String end = redisTemplate.opsForValue().get(endDateKey(couponId));
        if (start == null || end == null) {
            throw new CouponException(CouponErrorCode.NOT_FOUND);
        }
        LocalDateTime now = LocalDateTime.now();
        if (LocalDateTime.parse(start).isAfter(now) || LocalDateTime.parse(end).isBefore(now)) {
            throw new CouponException(CouponErrorCode.NOT_AVAILABLE);
        }
    }

    /** 잔여 재고. 키가 없으면 empty — 호출 측에서 DB fallback */
    public Optional<Integer> getStock(Long couponId) {
        return Optional.ofNullable(redisTemplate.opsForValue().get(stockKey(couponId)))
                .map(Integer::parseInt);
    }

    /**
     * 중복 확인 → 재고 차감 → 발급 이벤트 Stream 기록을 Lua 한 번으로 실행한다 (issue-coupon.lua).
     *
     * @return 예약 후 남은 재고
     * @throws CouponException DUPLICATED / EXHAUSTED / NOT_FOUND
     */
    public int reserve(Long couponId, Long userId, LocalDateTime issuedAt) {
        Long result = redisTemplate.execute(
                issueCouponScript,
                List.of(stockKey(couponId), usersKey(couponId), eventsKey(couponId)),
                String.valueOf(couponId), String.valueOf(userId), issuedAt.toString()
        );
        if (result == null) {
            throw new IllegalStateException("issue-coupon.lua가 결과를 반환하지 않았습니다");
        }
        if (result == RESULT_DUPLICATED) {
            throw new CouponException(CouponErrorCode.DUPLICATED);
        }
        if (result == RESULT_EXHAUSTED) {
            throw new CouponException(CouponErrorCode.EXHAUSTED);
        }
        if (result == RESULT_NOT_INITIALIZED) {
            throw new CouponException(CouponErrorCode.NOT_FOUND);
        }
        return result.intValue();
    }

    /** relay가 순회할, 이벤트 Stream이 등록된 쿠폰 ID 목록 */
    public Set<Long> getStreamCouponIds() {
        Set<String> members = redisTemplate.opsForSet().members(STREAM_INDEX_KEY);
        if (members == null) {
            return Set.of();
        }
        return members.stream().map(Long::valueOf).collect(Collectors.toSet());
    }

    /**
     * 발급 이벤트를 consumer group으로 읽는다.
     * 이 consumer에게 전달됐지만 아직 XACK 되지 않은 이벤트(PEL)를 먼저 돌려주므로,
     * relay가 Kafka 발행 도중 죽었다가 같은 이름으로 돌아오면 멈춘 곳부터 이어서 처리한다.
     */
    public List<MapRecord<String, Object, Object>> readEvents(Long couponId, String consumerName, int count) {
        List<MapRecord<String, Object, Object>> pending = read(couponId, consumerName, count, ReadOffset.from("0"));
        if (!pending.isEmpty()) {
            return pending;
        }
        return read(couponId, consumerName, count, ReadOffset.lastConsumed());
    }

    @SuppressWarnings("unchecked") // StreamOffset<K>... 가변 인자의 제네릭 배열 경고
    private List<MapRecord<String, Object, Object>> read(Long couponId, String consumerName, int count, ReadOffset offset) {
        List<MapRecord<String, Object, Object>> records = redisTemplate.opsForStream().read(
                Consumer.from(STREAM_GROUP, consumerName),
                StreamReadOptions.empty().count(count),
                StreamOffset.create(eventsKey(couponId), offset)
        );
        return records == null ? List.of() : records;
    }

    /** Kafka 브로커 ACK를 받은 이벤트를 처리 완료로 표시한다 (XACK) */
    public void acknowledge(Long couponId, RecordId recordId) {
        redisTemplate.opsForStream().acknowledge(eventsKey(couponId), STREAM_GROUP, recordId);
    }
}
