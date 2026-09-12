# Phase 3. Kafka로 비동기 Write-Back
## 선착순 쿠폰 시스템 구축기 (4) — DB 보호와 순서 보장

> Phase 2에서 Redis로 응답 속도를 잡았다.  
> 그런데 1000명 중 100명만 성공하는 구조에서 DB는 여전히 순간 폭발을 맞는다.  
> 게다가 Redis가 죽으면? 발급 이력이 날아간다.  
> Kafka를 Redis와 DB 사이에 두면 이 두 문제가 동시에 해결된다.

---

## 1. Phase 2의 남은 병목

### 1-1. Tomcat 스레드 풀

Phase 2 결과 p(95)=1.39s. 목표(500ms)에 못 미쳤다.

Redis 로직은 μs 단위로 빠른데 왜 응답이 느린가?

```
1000개 요청 동시 진입
         ↓
Tomcat 스레드 풀 (기본 200개)
         ↓
200개 → 즉시 처리 시작
800개 → 스레드 대기
         ↑
Redis가 아무리 빠르더라도 스레드를 못 받으면 의미 없다
```

`server.tomcat.threads.max=1000`으로 올리면 단기적으로 p95가 개선된다.  
그런데 스레드 1000개가 동시에 DB 쓰기를 시도하면?

```
100개 발급 성공 → 동시에 MySQL INSERT 100건 (순간 폭발)
         ↓
MySQL I/O 부하 스파이크
         ↓
다른 조회 쿼리까지 느려짐
```

Phase 2의 Redis 처리(DECR/SADD)는 빠르지만 DB 쓰기가 여전히 동기적이다.  
성공한 100개가 DB에 도달하는 시점이 몰리면 DB가 스파이크를 맞는다.

### 1-2. Redis 장애 시 유실

Redis는 기본적으로 메모리 기반이다.

```
시나리오:
1. 유저 A가 Redis에서 발급 성공 (SADD 1, DECR 99)
2. Redis가 프로세스 죽음 (OOM, 서버 재시작)
3. DB에 INSERT 되기 전이었다면 → 재고는 99인데 DB에는 기록 없음
4. Redis 재시작 → 재고 키 없음 → initStock 미호출 시 혼돈
```

RDB 스냅샷(`.rdb`) 또는 AOF(Append-Only File)로 Redis를 영속화할 수 있지만,  
"Redis 재고 카운터"와 "MySQL 발급 이력"이 항상 일치한다는 보장이 없다.

### 1-3. 순서 보장이 안 된다

Phase 2에서는 Redis DECR의 반환값이 "몇 번째 발급인지"를 나타내지만,  
이 값을 DB에 저장하지 않는다.

```
실서비스 요구사항 예시:
- "100번째 구매자에게 추가 선물"
- "가장 먼저 구매한 10명 VIP 등급 부여"
```

Redis DECR 순서와 MySQL INSERT 순서가 다를 수 있다.  
100개 요청이 동시에 들어오면 DECR이 먼저 끝난 요청이라도 DB INSERT는 나중에 커밋될 수 있다.

→ Phase 3에서 Kafka로 세 문제를 모두 해결한다.

---

## 2. Kafka란 무엇인가

### 2-1. 메시지 큐 vs 이벤트 스트리밍

기존 메시지 큐(RabbitMQ 등)와 Kafka의 근본적 차이를 알아야 한다.

**메시지 큐 (RabbitMQ):**
```
Producer → Queue → Consumer
                    ↑ 소비하면 큐에서 사라짐
                    ↑ Consumer가 처리 실패해도 DLQ(Dead Letter Queue)에서 재처리
```

**Kafka (이벤트 스트리밍):**
```
Producer → Topic (Log) → Consumer Group A
                       → Consumer Group B
                    ↑ 소비해도 로그에 남음
                    ↑ Consumer가 어디까지 읽었는지 Offset으로 추적
                    ↑ 재처리 시 Offset을 되감으면 됨
```

Kafka의 핵심은 **이벤트를 지우지 않고 로그로 쌓는다**는 것이다.  
여러 Consumer가 같은 토픽을 독립적으로 읽을 수 있다.

### 2-2. Topic, Partition, Offset

```
Topic "coupon-issue"
  ┌─────────────────────────────────────────┐
  │ Partition 0                              │
  │ [msg0] [msg1] [msg2] [msg3] ...          │
  │  0      1      2      3    ← Offset      │
  └─────────────────────────────────────────┘
  ┌─────────────────────────────────────────┐
  │ Partition 1                              │
  │ [msg0] [msg1] [msg2] ...                 │
  └─────────────────────────────────────────┘
  ┌─────────────────────────────────────────┐
  │ Partition 2                              │
  │ [msg0] [msg1] ...                        │
  └─────────────────────────────────────────┘
```

**Topic**: 메시지를 담는 논리적 단위. "우편함" 이름.

**Partition**: 토픽을 물리적으로 나눈 단위.  
- 순서 보장은 **같은 Partition 내에서만** 된다.
- Partition이 늘면 병렬 처리가 가능해진다.

**Offset**: Partition 안에서 메시지의 순번. 단조증가.  
- Consumer는 자신이 어디까지 읽었는지 Offset으로 추적한다.  
- `offset=3`이면 "0, 1, 2, 3번 메시지까지 읽었다"

### 2-3. Producer

Producer는 Topic에 메시지를 쓰는 쪽이다.

```java
kafkaTemplate.send("coupon-issue", couponId.toString(), event);
```

두 번째 인자는 **Key**다. Key가 같은 메시지는 **항상 같은 Partition**에 들어간다.  
`couponId`를 Key로 쓰면 같은 쿠폰의 발급 이벤트가 항상 같은 Partition에서 순서대로 처리된다.

### 2-4. Consumer Group

```
Consumer Group "coupon-db-writer"
  ┌──────────────┐   ┌──────────────┐   ┌──────────────┐
  │ Consumer 1   │   │ Consumer 2   │   │ Consumer 3   │
  │ Partition 0  │   │ Partition 1  │   │ Partition 2  │
  └──────────────┘   └──────────────┘   └──────────────┘
  같은 그룹 안에서 각 Consumer는 다른 Partition을 담당 (병렬 처리)

Consumer Group "coupon-analytics"
  ┌──────────────────────────────────────────────────────┐
  │ Consumer 1 (Partition 0, 1, 2 전부 읽음)              │
  └──────────────────────────────────────────────────────┘
  다른 그룹은 같은 토픽을 독립적으로 읽을 수 있음
```

Consumer Group이 다르면 **같은 메시지를 각자 독립적으로** 처리한다.  
- DB 저장 Consumer Group  
- 분석용 Consumer Group  
- 알림 발송 Consumer Group  
...을 같은 토픽에 붙여서 관심사별로 분리할 수 있다.

### 2-5. Kafka가 "빠른" 진짜 이유

Kafka가 메모리 기반인 Redis보다 느릴 것 같지만 메시지 큐로서는 압도적이다.

**Sequential Write (순차 쓰기):**
```
HDD Random Write : ~100 IOPS
HDD Sequential Write: ~10,000 IOPS
SSD Sequential Write: ~300,000 IOPS
```

Kafka는 모든 메시지를 Append-Only로 디스크에 순차 쓰기한다.  
랜덤 I/O가 없으므로 SSD가 아닌 HDD로도 수백만 TPS가 나온다.

**Zero-Copy:**  
일반적인 네트워크 전송은 `Disk → Kernel Buffer → User Buffer → Socket Buffer → NIC`다.  
Kafka는 `sendfile()` 시스템 콜로 `Disk → Kernel Buffer → NIC`를 바로 연결한다.  
CPU 복사 횟수가 줄어 단일 브로커로 수Gbps 처리가 가능하다.

**Batching:**  
Producer가 메시지를 개별로 보내지 않고 배치로 묶어서 보낸다.  
네트워크 왕복 횟수가 줄어 Throughput이 극적으로 개선된다.

### 2-6. KRaft — ZooKeeper 없는 Kafka

기존 Kafka는 클러스터 메타데이터 관리에 ZooKeeper가 필요했다.  
Kafka 2.8+부터는 **KRaft** 모드로 ZooKeeper 없이 단독 실행이 가능하다.

```yaml
# docker-compose.yml (Phase 3)
kafka:
  image: apache/kafka:3.7.1
  environment:
    KAFKA_PROCESS_ROLES: "broker,controller"   # 단일 노드가 둘 다 담당
    KAFKA_NODE_ID: "1"
    KAFKA_CONTROLLER_QUORUM_VOTERS: "1@kafka:9093"
```

이 프로젝트는 단일 노드 개발 환경이므로 KRaft가 적합하다.  
프로덕션에서는 Controller 3개 + Broker N개로 분리한다.

---

## 3. Phase 3 아키텍처

### 3-1. 전체 흐름

```
Phase 2:
클라이언트 → Spring → Redis (SADD + DECR)
                    → MySQL (INSERT, 동기)  ← 100건 동시 INSERT

Phase 3:
클라이언트 → Spring → Redis (SADD + DECR)
                    → Kafka (이벤트 발행) → 즉시 202 Accepted 반환
                                  ↓
                            Kafka Consumer (비동기)
                                  ↓
                            MySQL (INSERT, 순차)  ← DB 보호됨
```

### 3-2. 202 Accepted 패턴

Phase 3에서 API 응답이 바뀐다.

| 상황 | Phase 2 | Phase 3 |
|------|---------|---------|
| 재고 소진 | 409 즉시 | 409 즉시 |
| 중복 발급 | 409 즉시 | 409 즉시 |
| 발급 성공 | 200 (DB INSERT 완료 후) | **202** (Kafka 발행 완료, DB는 비동기) |

202 Accepted는 "요청을 받아 처리 중이지만 아직 완료되지 않았다"는 HTTP 표준 응답 코드다.  
유저 입장에서는 거의 즉시 응답을 받고, DB 저장은 백그라운드에서 진행된다.

### 3-3. Partition 수와 순서 보장

```
쿠폰 ID를 Kafka Key로 사용
→ 쿠폰 ID=1의 모든 발급 이벤트 → Partition 0
→ 쿠폰 ID=2의 모든 발급 이벤트 → Partition 1

Partition 0: [userId=100, seq=1] [userId=42, seq=2] [userId=7, seq=3] ...
→ Consumer가 순서대로 읽어서 DB에 순차 INSERT
→ seq(발급 순서)가 정확히 보장됨
```

**왜 Partition 내에서만 순서가 보장되는가:**  
Kafka는 Partition 단위로 독립적인 로그 파일을 관리한다.  
Partition 0의 msg1이 Partition 1의 msg1보다 먼저 발행됐어도,  
Consumer가 어느 Partition을 먼저 읽느냐에 따라 처리 순서가 달라진다.  
같은 쿠폰 이벤트를 같은 Partition에 보내면 쿠폰 단위 순서가 보장된다.

### 3-4. 컨슈머 처리 속도와 DB 보호

```
1000명 동시 요청 → Redis에서 900개 탈락 → 100개가 Kafka 발행 (μs 단위)
                                                    ↓
                                            Consumer: 1건씩 MySQL INSERT
                                            → DB는 한 번에 1건만 처리
                                            → 부하 평탄화(Smoothing)
```

이게 Phase 3의 핵심이다.  
Redis 처리가 완료되면 즉시 응답하고, DB 쓰기는 Consumer가 자신의 페이스로 처리한다.  
순간 폭발이 아닌 균일한 처리가 DB에 도달한다.

---

## 4. At-least-once vs Exactly-once

Kafka에서 가장 중요한 개념이다.

### 4-1. At-least-once (최소 한 번)

```
Producer → Kafka → Consumer → DB INSERT

Consumer가 DB INSERT는 성공했지만 Kafka에 Offset 커밋 전에 죽으면?
→ Kafka: "이 메시지 아직 안 처리된 것" → 재전송
→ Consumer 재시작: 같은 메시지 다시 받음 → DB INSERT 또 시도
→ 같은 유저에게 쿠폰 2개 발급!
```

`enable.auto.commit=true` (기본값)이면 이런 상황이 발생할 수 있다.

### 4-2. Exactly-once (정확히 한 번)

Kafka는 Producer와 Consumer 양쪽에 Exactly-once 옵션이 있다.

**Producer Side:**
```yaml
spring:
  kafka:
    producer:
      enable-idempotence: true   # 중복 전송 방지
      acks: all                   # 모든 replica가 저장 확인 후 응답
      retries: 3
```

**Consumer Side (DB와 Offset을 같은 트랜잭션에 묶기):**
```java
@Transactional
@KafkaListener(topics = "coupon-issue")
public void consume(CouponIssueEvent event) {
    issuedCouponRepository.save(...);
    // @Transactional 내에서 DB + Offset 커밋을 atomic하게
}
```

단, `isolation.level=read_committed` 설정과 `Kafka Transaction` 설정이 필요하다.

### 4-3. 이 프로젝트의 선택 — Idempotent Consumer

완전한 Exactly-once는 설정이 복잡하다.  
실무에서 많이 쓰는 방법은 **At-least-once + Idempotent Consumer**다.

```java
@Transactional
@KafkaListener(topics = "coupon-issue")
public void consume(CouponIssueEvent event) {
    // 이미 처리된 이벤트인지 확인 (Idempotency Key)
    boolean alreadyIssued = issuedCouponRepository
        .existsByCouponIdAndUserId(event.getCouponId(), event.getUserId());
    
    if (alreadyIssued) {
        log.warn("Duplicate event: coupon={}, user={}", event.getCouponId(), event.getUserId());
        return;  // 중복이면 그냥 넘어감 — 幂等(멱등) 처리
    }
    
    issuedCouponRepository.save(...);
}
```

중복으로 메시지가 오더라도 DB를 확인해서 이미 처리됐으면 무시한다.  
**멱등(Idempotent)** — 같은 연산을 여러 번 해도 결과가 바뀌지 않음.

---

## 5. Consumer Lag — 지연 모니터링

### 5-1. Consumer Lag이란

```
Partition:  [0] [1] [2] [3] [4] [5] [6] [7]  ← 최신 메시지 (Offset 7)
                                   ↑
                            Consumer 현재 위치 (Offset 5)
                            
Consumer Lag = 7 - 5 = 2  (아직 처리 못 한 메시지 수)
```

Consumer Lag이 증가한다면:
- 발급 이벤트가 Kafka에는 쌓였는데 DB에는 아직 반영 안 됨
- "발급 성공인데 내 쿠폰 목록에 없어요" 민원 발생 가능

Lag이 0에 가까워야 DB와 Kafka가 거의 실시간으로 동기화된 상태다.

### 5-2. Kafka 모니터링

실무에서는 Kafka Exporter + Prometheus + Grafana로 Lag을 시각화한다.

```yaml
# docker-compose에 추가 가능
kafka-exporter:
  image: danielqsj/kafka-exporter
  command: ["--kafka.server=kafka:9094"]
  ports:
    - "9308:9308"
```

이 프로젝트에서는 kafka-ui로 간단하게 확인할 수 있다.

```yaml
kafka-ui:
  image: provectuslabs/kafka-ui:latest
  ports:
    - "8989:8080"
  environment:
    KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:9094
```

---

## 6. Phase 3 구현 상세

### 6-1. 의존성 추가

```gradle
// build.gradle
implementation 'org.springframework.kafka:spring-kafka'
```

### 6-2. application.yml 설정

```yaml
spring:
  kafka:
    bootstrap-servers: ${SPRING_KAFKA_BOOTSTRAP_SERVERS:localhost:9092}
    
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.springframework.kafka.support.serializer.JsonSerializer
      acks: all              # 모든 replica 저장 확인
      retries: 3             # 실패 시 재시도
      enable-idempotence: true  # 중복 전송 방지
      
    consumer:
      group-id: coupon-db-writer
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.JsonDeserializer
      auto-offset-reset: earliest   # 처음부터 읽기 (개발 환경)
      enable-auto-commit: false      # 수동 Offset 커밋
      properties:
        spring.json.trusted.packages: "com.example.coupon.event"
```

### 6-3. 이벤트 객체

```java
// CouponIssueEvent.java
package com.example.coupon.event;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@NoArgsConstructor
@AllArgsConstructor
public class CouponIssueEvent {
    private Long couponId;
    private Long userId;
    private String requestedAt;  // 발급 요청 시각 (ISO-8601)
}
```

### 6-4. CouponKafkaProducer

```java
@Service
@RequiredArgsConstructor
public class CouponKafkaProducer {

    private static final String TOPIC = "coupon-issue";
    private final KafkaTemplate<String, CouponIssueEvent> kafkaTemplate;

    public void publish(Long couponId, Long userId) {
        CouponIssueEvent event = new CouponIssueEvent(
            couponId, userId, LocalDateTime.now().toString()
        );
        // couponId를 Key로 사용 → 같은 쿠폰 이벤트는 같은 Partition
        kafkaTemplate.send(TOPIC, couponId.toString(), event);
    }
}
```

### 6-5. CouponService 변경 (issueCoupon)

```java
// Phase 2 → Phase 3 변경점
public void issueCoupon(Long couponId, Long userId) {
    // Step 1. 기간 체크 (Redis) — 변경 없음
    couponRedisService.validatePeriod(couponId);

    // Step 2. 중복 체크 + 재고 차감 (Redis) — 변경 없음
    couponRedisService.checkAndIssue(couponId, userId);

    // Step 3. Kafka 이벤트 발행 (Phase 2의 DB INSERT 대체)
    //   → 여기서 즉시 반환. DB 쓰기는 Consumer가 비동기로 처리.
    couponKafkaProducer.publish(couponId, userId);
}
```

Phase 2와 비교:
- Phase 2: Step 3에서 `TransactionTemplate`으로 MySQL INSERT (동기)
- Phase 3: Step 3에서 Kafka 이벤트 발행 → 즉시 반환 (비동기)

### 6-6. CouponController 응답 코드 변경

```java
@PostMapping("/{couponId}/issue")
public ResponseEntity<ApiResponse<?>> issueCoupon(
    @PathVariable Long couponId,
    @RequestBody IssueCouponRequest request
) {
    couponService.issueCoupon(couponId, request.getUserId());
    // 200 → 202: "요청 접수, 처리 중"
    return ResponseEntity.accepted()
        .body(ApiResponse.ok("쿠폰 발급 요청이 접수되었습니다."));
}
```

### 6-7. CouponKafkaConsumer

```java
@Service
@RequiredArgsConstructor
public class CouponKafkaConsumer {

    private final CouponRepository couponRepository;
    private final IssuedCouponRepository issuedCouponRepository;

    // ─────────────────────────────────────────────────────────
    // Kafka 이벤트 소비 → MySQL INSERT
    //
    // @Transactional: DB INSERT 실패 시 롤백
    // Offset 커밋은 @KafkaListener가 자동으로 처리
    //   → enable-auto-commit=false이면 처리 성공 후 커밋
    // ─────────────────────────────────────────────────────────
    @Transactional
    @KafkaListener(topics = "coupon-issue", groupId = "coupon-db-writer")
    public void consume(CouponIssueEvent event) {
        Long couponId = event.getCouponId();
        Long userId   = event.getUserId();

        // 멱등성 체크 — 중복 재전송 대비
        if (issuedCouponRepository.existsByCouponIdAndUserId(couponId, userId)) {
            return;
        }

        Coupon coupon = couponRepository.findById(couponId)
            .orElseThrow(() -> new RuntimeException("Coupon not found: " + couponId));

        couponRepository.incrementIssuedQuantity(couponId);

        LocalDateTime now = LocalDateTime.parse(event.getRequestedAt());
        IssuedCoupon issuedCoupon = new IssuedCoupon();
        issuedCoupon.setCoupon(coupon);
        issuedCoupon.setUserId(userId);
        issuedCoupon.setStatus(com.example.coupon.domain.issuedCoupon.Status.ISSUED);
        issuedCoupon.setIssuedAt(now);
        issuedCoupon.setExpiredAt(now.plusDays(coupon.getValidDays()));
        issuedCouponRepository.save(issuedCoupon);
    }
}
```

### 6-8. Kafka 에러 처리 — Dead Letter Topic

Consumer가 3번 재시도해도 실패하면 메시지를 DLT(Dead Letter Topic)로 보낸다.

```java
@Bean
public DefaultErrorHandler errorHandler(KafkaTemplate<String, Object> kafkaTemplate) {
    // 3번 재시도 후 DLT로 전송
    DeadLetterPublishingRecoverer recoverer =
        new DeadLetterPublishingRecoverer(kafkaTemplate,
            (record, ex) -> new TopicPartition("coupon-issue.DLT", record.partition()));
    
    FixedBackOff backOff = new FixedBackOff(1000L, 3L); // 1초 간격, 3회
    return new DefaultErrorHandler(recoverer, backOff);
}
```

DLT에 쌓인 실패 메시지는:
1. 별도 알림 (Slack 등)으로 확인
2. 원인 수정 후 DLT Consumer로 재처리
3. 수동 처리 (최후 수단)

---

## 7. Redis-Kafka-MySQL 정합성 문제

### 7-1. 발급 성공인데 DB에 없는 경우

```
타임라인:
T=0: Redis checkAndIssue 성공 (stock 99, users에 userId 추가)
T=1: Kafka 이벤트 발행 성공
T=2: Consumer 처리 중...
T=3: 유저가 "내 쿠폰 목록" API 호출 → DB 조회 → 아직 없음!
```

202 Accepted를 반환했으므로 클라이언트가 "처리 중"임을 인지해야 한다.  
실무에서는 다음 중 하나로 처리한다:

**방법 A: 폴링**
```javascript
// 클라이언트가 주기적으로 확인
async function pollIssuedCoupons() {
    while (true) {
        const coupons = await fetchMyCoupons();
        if (coupons.find(c => c.id === targetCouponId)) break;
        await sleep(500);
    }
}
```

**방법 B: Redis를 발급 완료 플래그로 사용**
```java
// Consumer가 DB INSERT 완료 후 Redis에 완료 플래그 저장
redisTemplate.opsForValue().set(
    "coupon:" + couponId + ":issued:" + userId, "1", 1, TimeUnit.HOURS
);
```

이 프로젝트에서는 Consumer Lag이 매우 짧아서 (수십 ms 이내) 클라이언트가 재조회하면 대부분 이미 처리 완료다.

### 7-2. Kafka 발행 실패

Redis는 성공했는데 Kafka 발행이 실패하면?

```
T=0: Redis checkAndIssue 성공 → stock 99, userId 추가됨
T=1: Kafka 발행 실패 (브로커 장애 등)
T=2: 발급 이벤트 유실 → MySQL에는 기록 없음
     Redis에는 userId가 있어서 다시 발급 시도하면 "중복" 에러
     → 사용자가 발급받은 것처럼 처리됐는데 실제로는 안 됨
```

이걸 방지하는 패턴이 **Outbox Pattern**이다:

```java
@Transactional
public void issueCoupon(Long couponId, Long userId) {
    couponRedisService.validatePeriod(couponId);
    couponRedisService.checkAndIssue(couponId, userId);
    
    // Kafka 발행 대신 DB Outbox 테이블에 INSERT (같은 트랜잭션)
    outboxRepository.save(new OutboxEvent("coupon-issue", couponId, userId));
}

// 별도 스케줄러 or CDC(Change Data Capture)가 Outbox를 읽어서 Kafka 발행
```

DB 트랜잭션으로 Outbox INSERT가 보장되므로 "Redis 성공 + Kafka 미발행" 상태가 없어진다.  
이 프로젝트에서는 학습 목적으로 Outbox Pattern 없이 진행하고 한계로 인식한다.

---

## 8. Phase 2 vs Phase 3 성능 비교

| 지표 | Phase 1 (DB Lock) | Phase 2 (Redis) | Phase 3 (Redis + Kafka) |
|------|-------------------|-----------------|--------------------------|
| 평균 응답시간 | 1.2s | 947ms | ~50ms (목표) |
| p(95) | 2.32s | 1.39s | ~100ms (목표) |
| RPS | 409/s | 589/s | 1000+/s (목표) |
| 발급 정확도 | 100/1000 ✅ | 100/1000 ✅ | 100/1000 ✅ |
| DB 즉시 정합성 | ✅ | ✅ | ❌ (최종 일관성) |
| DB 쓰기 방식 | 동기 (직접) | 동기 (직접) | **비동기 (Consumer)** |

**Phase 3에서 응답이 빠른 이유:**
```
Phase 2: Redis(μs) → MySQL INSERT(수십ms) → 응답
Phase 3: Redis(μs) → Kafka 발행(μs) → 즉시 응답 → [나중에 MySQL]
```

응답 시간에서 "MySQL INSERT 대기 시간"이 제거된다.

---

## 9. Phase 3의 한계 (실서비스에서 더 필요한 것)

### 9-1. Redis 단일 장애점

Redis가 죽으면 전체 쿠폰 발급이 중단된다.  
실서비스에서는 Redis Sentinel(HA) 또는 Redis Cluster로 이중화한다.

```
Redis Sentinel:
Master ─── Slave 1
       └── Slave 2
  ↑
Sentinel이 Master 장애 감지 → Slave 1을 Master로 자동 승격
```

### 9-2. Kafka 브로커 단일 장애점

이 프로젝트는 단일 Kafka 브로커다.  
프로덕션에서는 Replication Factor=3으로 설정하고, 브로커 3대 이상 운영한다.

```yaml
KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: "3"  # 브로커 3대
KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: "3"
```

### 9-3. 앞단 쓰로틀링 부재

Phase 3까지도 1만 명이 동시에 접근하면 Tomcat 스레드 → Redis까지는 모두 부하를 받는다.  
실서비스에서는 **대기열(Queue)**을 앞단에 두어 트래픽 자체를 통제한다.

```
100만 명 동시 접근
→ 대기열 서버 (WebSocket/SSE 기반)
→ 10초마다 1000명씩만 "입장 허가"
→ 뒷단(Spring + Redis + Kafka)은 항상 허용 범위 내 트래픽
```

이 구조가 웍스아웃, 피스마이너스원이 선택한 방향이다.

### 9-4. 최종 일관성 (Eventual Consistency)

Phase 3는 Redis와 MySQL이 항상 일치하지 않는다.  
Kafka Consumer가 처리 지연되면 "발급은 됐는데 쿠폰 목록에 없음" 상황이 잠시 발생한다.

이를 허용 범위로 설계해야 한다.  
"쿠폰 발급 후 몇 초 이내에 쿠폰 목록에 반영된다"는 SLA를 정의하고,  
그 안에 Consumer가 처리하도록 Consumer Lag을 모니터링한다.

---

## 10. 아키텍처 진화 전체 회고

```
Phase 1: DB Pessimistic Lock
   핵심 아이디어: "한 번에 한 명씩 처리"
   문제: DB 커넥션 풀 고갈, 응답 지연

          ↓ DB에서 Redis로 제어권 이동

Phase 2: Redis 원자적 연산
   핵심 아이디어: "DB Lock 없이 원자성 확보"
   문제: DB 쓰기가 여전히 동기, Tomcat 스레드 병목

          ↓ 동기 DB 쓰기를 비동기로

Phase 3: Redis Gate + Kafka Write-Back
   핵심 아이디어: "결과는 나중에 쓰고, 응답은 지금"
   문제: 최종 일관성, Redis/Kafka 장애 대응

          ↓ 앞단 쓰로틀링 + 분산 아키텍처

실서비스: 대기열 + Redis Cluster + Kafka 클러스터 + Outbox Pattern
```

각 Phase는 이전 Phase의 병목을 정확하게 찾아서 해결했다.  
완전한 해결책을 한 번에 설계하는 것보다,  
**현재 병목이 어디인지 측정하고 거기만 고치는 것**이 엔지니어링의 본질이다.

---

## 다음 단계

Phase 3 구현 후 실제 측정값으로 표를 채우고,  
Consumer Lag을 Grafana에서 시각화하면 Phase 1 → 2 → 3 진화를 한눈에 볼 수 있다.

> 측정하지 않으면 개선이 아니라 추측이다.

```sh
 █ THRESHOLDS

    http_req_duration
    ✗ 'p(95)<300' p(95)=2.03s

    http_req_failed
    ✓ 'rate<0.95' rate=90.00%


  █ TOTAL RESULTS

    checks_total.......: 3000   1409.601548/s
    checks_succeeded...: 66.66% 2000 out of 3000
    checks_failed......: 33.33% 1000 out of 3000

    ✗ status 200 or 202 (발급 성공/접수)
      ↳  10% — ✓ 100 / ✗ 900
    ✗ status 409 (소진/중복)
      ↳  90% — ✓ 900 / ✗ 100
    ✓ status is not 500

    CUSTOM
    coupon_issue_fail..............: 900    422.880464/s
    coupon_issue_success...........: 100    46.986718/s

    HTTP
    http_req_duration..............: avg=732.08ms min=89.12ms  med=1.06s    max=2.04s    p(90)=1.12s    p(95)=2.03s
      { expected_response:true }...: avg=336.83ms min=106.65ms med=348.03ms max=513.91ms p(90)=469.27ms p(95)=487.17ms
    http_req_failed................: 90.00% 900 out of 1000
    http_reqs......................: 1000   469.867183/s

    EXECUTION
    iteration_duration.............: avg=795.43ms min=92.88ms  med=1.12s    max=2.11s    p(90)=1.2s     p(95)=2.1s
    iterations.....................: 1000   469.867183/s
    vus............................: 62     min=62          max=526
    vus_max........................: 1000   min=1000        max=1000

    NETWORK
    data_received..................: 268 kB 126 kB/s
    data_sent......................: 156 kB 73 kB/s
```

```
⏺ ┌─────────┐
  │ Client  │
  └────┬────┘
       │ POST /api/coupons/{id}/issue
       ▼
  ┌─────────────────────────────────────────────┐
  │              CouponController               │
  │                                             │
  │  couponService.issueCoupon(couponId, userId)│
  │  return 202 Accepted ◄──────────────────── │
  └────────────────┬────────────────────────────┘
                   │
                   ▼
  ┌─────────────────────────────────────────────┐
  │              CouponService                  │
  │                                             │
  │  Step 1. DB에서 쿠폰 유효성 검사            │
  │  Step 2. Redis Lua Script (원자적 실행)     │
  │    ├─ SADD coupon:{id}:users {userId}       │
  │    │    → 0이면 중복 → 예외                 │
  │    └─ DECR coupon:{id}:stock               │
  │         → 음수면 재고 소진 → 예외           │
  │  Step 3. Kafka에 이벤트 발행               │
  └──────┬───────────────────┬─────────────────┘
         │                   │
  │  Step 3. Kafka에 이벤트 발행               │
  │  Step 3. Kafka에 이벤트 발행               │
  └──────┬───────────────────┬─────────────────┘
         │                   │
         ▼                   ▼
  ┌─────────────┐   ┌─────────────────────┐
  │    Redis    │   │        Kafka        │
  │             │   │  topic: coupon-issue│
  │ stock: 999  │   │  key: {couponId}    │
  │ users: Set  │   │  (같은 파티션 보장) │
  └─────────────┘   └──────────┬──────────┘
                                │ 비동기 소비
                                ▼
                    ┌───────────────────────┐
                    │   CouponKafkaConsumer │
                    │                       │
                    │  1. 멱등성 체크 (DB)  │
                    │  2. issued_quantity+1 │
                    │  3. issued_coupon INSERT│
                    │  4. Kafka offset 커밋 │
                    └──────────┬────────────┘
                                │
                                ▼
                         ┌─────────────┐
                         │    MySQL    │
                         │  coupon     │
                         │  issued_coupon│
                         └─────────────┘
```
  
  핵심 포인트:

                                                                       - Redis: 재고 차감 + 중복 체크를 Lua Script로 동기적·원자적 처리 → 202 반환 전에
  완료
  - Kafka: DB 저장을 비동기로 분리 → API 응답 속도 향상
  - 202 Accepted: Redis 통과 = "발급 확정", MySQL INSERT는 Consumer가 나중에 처리
  - 정합성 위험: Redis users 수와 DB issued_coupon 수가 일시적으로 다를 수 있음 →
  report.sh의 정합성 체크가 이걸 잡는 것
