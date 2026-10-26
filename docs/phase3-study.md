# Phase 3. Kafka로 비동기 Write-Back
## 선착순 쿠폰 시스템 구축기 (4) — DB 보호와 순서 보장

> **2026-10-22 정리:** 이 문서는 구현 과정의 학습 예시와 당시 측정 기록을 포함한다. 기존 Phase 3은 Redis `SADD` → `DECR`을 별도 호출하며, `KafkaTemplate.send()`의 완료를 기다리지 않고 `202`를 반환한다. Lua, Outbox, DLT, 요청 상태 조회는 기존 Phase 3에 없다. 후속 [실험 모듈](../5-coupon_experiments/README.md)에는 Lua·Outbox·Stream·상태 조회와 Kafka 우선 경로를 구현했다. DLT는 여전히 미구현이다. 현재 4단계 [4-coupon_redisHA_kafka](../4-coupon_redisHA_kafka/README.md)는 Lua·Stream 없이 Phase 3에 Kafka ACK 확인과 보상, DB 유니크 제약을 더한 최소 구성이다. 설계 질문은 [멘토링 논의안](mentoring-discussion.md), 측정·장애·순서 결과는 [실증 보고서](experiments-2026-10-22.md)를 기준으로 읽는다.

> Phase 2에서 Redis로 응답 속도를 잡았다.  
> 그런데 1000명 중 100명만 성공하는 구조에서 DB는 여전히 순간 폭발을 맞는다.  
> 게다가 Redis가 죽으면? 발급 이력이 날아간다.  
> Kafka를 Redis와 DB 사이에 두면 DB 쓰기를 비동기로 분리할 수 있다. Redis 장애와 Redis–Kafka 사이의 유실 구간은 별도 복구 설계가 필요하다.

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

Phase 2의 Redis DECR 반환값은 차감 직후 잔여 재고다. 보상으로 재고가 증가할 수 있으므로 영구적인 발급 순번으로 사용할 수 없고, 현재 코드는 이 값을 DB에 저장하지 않는다.

```
실서비스 요구사항 예시:
- "100번째 구매자에게 추가 선물"
- "가장 먼저 구매한 10명 VIP 등급 부여"
```

Redis DECR 순서와 MySQL INSERT 순서가 다를 수 있다.  
100개 요청이 동시에 들어오면 DECR이 먼저 끝난 요청이라도 DB INSERT는 나중에 커밋될 수 있다.

→ Phase 3에서는 DB 쓰기를 분리한다. Kafka에 기록되기 전의 Redis 처리 순서와 장애 복구까지 자동으로 해결되는 것은 아니다.

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

두 번째 인자는 **Key**다. 동일한 파티셔너와 파티션 수를 유지하면 같은 키는 같은 Partition에 배치된다. 파티션 증설 시 키의 매핑이 달라질 수 있으므로 진행 중인 쿠폰의 순서를 유지할 전환 정책이 필요하다.

`couponId`를 키로 묶어도 보장되는 기준은 **그 Partition에 기록된 순서**다. 여러 API 서버의 요청 도착 순서나 Redis 차감 순서를 복원하지는 않는다. Producer의 재시도 순서도 idempotence와 in-flight 설정에 영향을 받는다. [Kafka Producer 설정](https://kafka.apache.org/37/generated/producer_config.html)

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
                    → Kafka 전송 요청 → 202 Accepted 반환 (전송 완료 대기 없음)
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
| Redis 선점 통과 후 응답 | 200 (DB INSERT 완료 후) | **202** (Kafka 전송 요청 후, 기록 성공·DB 완료는 미확인) |

202 Accepted는 "요청을 받아 처리 중이지만 아직 완료되지 않았다"는 HTTP 표준 응답 코드다.  
DB 저장 대기를 응답 경로에서 분리하지만, 실제 발급 결과는 별도로 확인해야 한다.

### 3-3. Partition 수와 순서 보장

```
쿠폰 ID를 Kafka Key로 사용 (아래 파티션 배치는 예시; 서로 다른 키도 같은 파티션에 배치될 수 있음)
→ 쿠폰 ID=1의 모든 발급 이벤트 → Partition 0
→ 쿠폰 ID=2의 모든 발급 이벤트 → Partition 1

Partition 0: [userId=100, seq=1] [userId=42, seq=2] [userId=7, seq=3] ...
→ 같은 Partition을 직렬 처리하고 앞선 처리가 완료된 뒤 다음 처리
→ Kafka 기록 순서에 맞춘 DB 반영 가능 (위 seq는 설명용이며 현재 이벤트 필드가 아님)
```

**왜 Partition 내에서만 순서가 보장되는가:**  
Kafka는 Partition 단위로 독립적인 로그 파일을 관리한다.  
Partition 0의 msg1이 Partition 1의 msg1보다 먼저 발행됐어도,  
Consumer가 어느 Partition을 먼저 읽느냐에 따라 처리 순서가 달라진다.  
같은 쿠폰 이벤트를 같은 Partition에 보내는 것은 순서를 지키기 위한 조건 중 하나다. Consumer 내부 병렬 처리, retry topic/DLT 우회, 리밸런싱 이후 미완료 작업의 뒤늦은 DB 반영도 통제해야 한다. 엄격한 선착순이라면 실패한 선행 요청을 기다릴지, 최종 탈락시킬지 정책을 먼저 정해야 한다. [순서와 장애에 대한 논의](mentoring-discussion.md)

### 3-4. 컨슈머 처리 속도와 DB 보호

```
1000명 동시 요청 → Redis에서 900개 탈락 → 100개가 Kafka 발행 시도
                                                    ↓
                                            Consumer: 1건씩 MySQL INSERT
                                            → DB는 한 번에 1건만 처리
                                            → 부하 평탄화(Smoothing)
```

이게 Phase 3의 핵심이다.  
현재 코드는 Redis 처리 후 Kafka 전송을 요청하고, DB 완료를 기다리지 않고 응답한다. 전송 함수도 메타데이터나 버퍼를 기다릴 수 있어 항상 즉시 반환하는 것은 아니다. Consumer 동시성과 DB 처리 한도를 조절해 쓰기 부하를 평준화한다.

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

이 중복 구간은 `enable.auto.commit=false`여도 존재한다. DB 커밋과 Kafka offset 커밋은 별개의 작업이기 때문이다. `false`는 Kafka 클라이언트의 주기적 자동 커밋을 끄는 설정이며, 실제 커밋 시점은 Spring의 `AckMode`와 에러 처리 정책에 달려 있다. [Spring Kafka offset 커밋](https://docs.spring.io/spring-kafka/reference/3.3/kafka/receiving-messages/message-listener-container.html)

### 4-2. Exactly-once (정확히 한 번)

Producer 재전송의 중복 제거와 Kafka 트랜잭션의 Exactly-once 범위는 구분해야 한다. 외부 MySQL 쓰기는 Kafka 설정만으로 exactly-once가 되지 않는다.

**Producer Side:**
```yaml
spring:
  kafka:
    producer:
      properties:
        enable.idempotence: true # Producer 자체 재전송의 중복 제거
      acks: all                   # 현재 ISR의 확인; 복제 수와 min.insync.replicas도 필요
      retries: 3
```

**Consumer Side (DB 트랜잭션과 Kafka 트랜잭션 구분):**
```java
@Transactional
@KafkaListener(topics = "coupon-issue")
public void consume(CouponIssueEvent event) {
    issuedCouponRepository.save(...);
    // DB 트랜잭션 매니저의 @Transactional은 DB 작업만 원자적으로 커밋한다.
    // Kafka offset 커밋과 하나의 원자적 트랜잭션이 되는 것은 아니다.
}
```

Kafka 트랜잭션은 Kafka로 쓰는 결과와 소비 offset을 함께 커밋하는 범위에서 사용한다. `read_committed`는 커밋된 Kafka 트랜잭션의 레코드를 읽게 할 뿐, MySQL을 그 트랜잭션에 포함하지 않는다. Spring에서 DB와 Kafka 트랜잭션을 동기화해도 커밋은 순차적으로 일어나며 두 번째 커밋 실패에 대한 복구가 필요하다. [Spring Kafka 트랜잭션](https://docs.spring.io/spring-kafka/reference/3.3/kafka/transactions.html)

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

다만 `exists` 조회 후 INSERT만으로는 병렬 재처리 경쟁을 막을 수 없다. 현재 엔티티에는 `(coupon_id, user_id)` 유니크 제약이 선언되어 있지 않다. 최종 방어선으로 DB 제약을 두고, 중복 이벤트일 때 발급 수 증가도 함께 건너뛰거나 롤백되도록 같은 DB 트랜잭션에서 처리해야 한다.

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

Lag은 Kafka 소비가 밀리는지를 보여준다. 에러 처리기가 실패 레코드를 건너뛰거나 DLT로 보내면 Lag이 줄어도 DB 반영은 누락될 수 있으므로 발급 이력과 실패 상태를 함께 대조해야 한다.

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

아래는 설정 학습 예시다. 현재 저장소는 Producer의 `acks`·`retries`·idempotence를 명시하지 않았으므로 실행에 적용되는 Kafka 클라이언트 버전과 유효 설정을 별도로 확인해야 한다.

```yaml
spring:
  kafka:
    bootstrap-servers: ${SPRING_KAFKA_BOOTSTRAP_SERVERS:localhost:9092}
    
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.springframework.kafka.support.serializer.JsonSerializer
      acks: all              # 현재 ISR의 확인
      retries: 3             # 실패 시 재시도
      properties:
        enable.idempotence: true # Producer 자체 재전송의 중복 제거
      
    consumer:
      group-id: coupon-db-writer
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.JsonDeserializer
      auto-offset-reset: earliest   # 처음부터 읽기 (개발 환경)
      enable-auto-commit: false      # 컨테이너의 AckMode에 따라 커밋
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
        // 같은 파티셔너와 파티션 수에서 couponId별로 같은 Partition에 배치
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
    //   → 전송 완료를 기다리지 않고 반환. DB 쓰기는 Consumer가 비동기로 처리.
    couponKafkaProducer.publish(couponId, userId);
}
```

Phase 2와 비교:
- Phase 2: Step 3에서 `TransactionTemplate`으로 MySQL INSERT (동기)
- Phase 3: Step 3에서 Kafka 전송 요청 → 전송 완료와 DB 완료를 기다리지 않고 반환

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
    //   → enable-auto-commit=false일 때 컨테이너의 AckMode에 따라 커밋
    //   → DB 커밋과 offset 커밋 사이에 종료되면 같은 이벤트를 재처리할 수 있음
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

아래는 **미구현 개선 예시**다. 설정하면 최초 시도 후 3회 재시도한 메시지를 DLT로 보낼 수 있다. 현재 코드에는 이 에러 처리기나 DLT가 없다. 설정이 없다고 무한 재시도가 보장되는 것은 아니며, 실제 재시도·복구·건너뛰기 동작은 적용되는 에러 처리기와 예외 분류를 확인해야 한다. [Spring Kafka 에러 처리](https://docs.spring.io/spring-kafka/reference/3.3/kafka/annotation-error-handling.html)

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

### 7-1. 접수 응답 후 DB에 아직 없는 경우

```
타임라인:
T=0: Redis checkAndIssue 성공 (stock 99, users에 userId 추가)
T=1: Kafka 이벤트 기록 성공 (이 타임라인의 가정; 현재 API는 완료를 기다리지 않음)
T=2: Consumer 처리 중...
T=3: 유저가 "내 쿠폰 목록" API 호출 → DB 조회 → 아직 없음!
```

202 Accepted를 반환했으므로 클라이언트가 "처리 중"임을 인지해야 한다.  
현재 `CouponKafkaProducer.publish()`는 `send()`의 Future를 기다리거나 결과 콜백을 등록하지 않는다. 따라서 `202` 자체가 Kafka 기록 성공이나 발급 확정을 뜻하지 않는다. [Spring Kafka 전송 결과 확인](https://docs.spring.io/spring-kafka/reference/3.3/kafka/sending-messages.html)

개선 시에는 안정적인 요청 ID로 접수·처리 중·성공·실패 상태를 조회하게 하고, 다음 방법을 검토한다:

**방법 A: 폴링**
```javascript
// 개념 예시: 실제로는 요청 ID 기반 상태 조회, 최대 대기 시간, 실패·재연결 처리 필요
async function pollIssuedCoupons() {
    while (true) {
        const coupons = await fetchMyCoupons();
        if (coupons.find(c => c.id === targetCouponId)) break;
        await sleep(500);
    }
}
```

**방법 B: DB 커밋 후 Redis에 완료 상태를 반영**
```java
// DB 커밋 완료 후 실행하는 개념 예시. DB 트랜잭션 내부에서 먼저 실행하면 안 됨.
redisTemplate.opsForValue().set(
    "coupon:" + couponId + ":issued:" + userId, "1", 1, TimeUnit.HOURS
);
```

DB 커밋 후 Redis 상태 기록이 실패하면 DB는 성공인데 상태는 처리 중으로 남을 수 있다. 상태 복구와 조회 시 대조가 필요하다. 현재는 전용 요청 상태 API가 없으며, 수십 ms 내 처리 완료를 보장하는 측정 근거도 없으므로 완료 지연의 p95/p99를 별도로 측정해야 한다.

### 7-2. Kafka 발행 실패

Redis는 성공했는데 Kafka 발행이 실패하면?

```
T=0: Redis checkAndIssue 성공 → stock 99, userId 추가됨
T=1: Kafka 미기록이 확정된 실패 (예: 전송 전 프로세스 종료)
T=2: 발급 이벤트 없음 → MySQL에는 기록 없음
     Redis에는 userId가 있어서 다시 발급 시도하면 "중복" 에러
     → 사용자가 발급받은 것처럼 처리됐는데 실제로는 안 됨
```

발행 timeout처럼 **기록 성공 여부를 모르는 경우**도 구분해야 한다. Kafka에 이미 기록된 요청의 Redis 재고를 바로 복원하면, 기존 이벤트의 발급과 새 요청의 재고 선점이 겹칠 수 있다. 요청 ID로 재발행을 멱등 처리하고, 상태를 확인한 뒤 보상해야 한다.

**DB Outbox만 추가해서 Redis 차감과 이벤트 기록이 원자적으로 묶이지는 않는다.** 다음 코드는 그 한계를 보여주는 예시다:

```java
@Transactional
public void issueCoupon(Long couponId, Long userId) {
    couponRedisService.validatePeriod(couponId);
    couponRedisService.checkAndIssue(couponId, userId);
    
    // DB Outbox INSERT만 DB 트랜잭션에 포함된다. 위 Redis 변경은 포함되지 않는다.
    outboxRepository.save(new OutboxEvent("coupon-issue", couponId, userId));
}

// 별도 스케줄러 or CDC(Change Data Capture)가 Outbox를 읽어서 Kafka 발행
```

Redis 차감 후 Outbox INSERT 전에 프로세스가 종료되거나 DB 트랜잭션이 롤백되면 여전히 공백이 생긴다. DB Outbox를 사용하려면 **DB에 권위 있는 예약/발급 상태와 Outbox를 같은 트랜잭션으로 기록**하고 Redis의 역할을 다시 정해야 한다. Redis에서 예약과 durable 전달 기록을 함께 만들고 relay로 옮기는 안도 검토할 수 있지만, Redis 영속성·failover와 relay 중복 처리 문제는 남는다.

기존 Phase 3에는 Outbox나 자동 복구가 없다. 후속 모듈의 DB Outbox와 Redis Stream 복구 비교는 [실증 보고서](experiments-2026-10-22.md)에 있다. 어떤 저장소를 운영의 원본으로 선택할지, 미처리 예약을 어떻게 대조할지는 [멘토링의 장애 복구 논의](mentoring-discussion.md)에서 결정한다.

---

## 8. 초기 개발 기록과 Phase 3 성능 목표

아래는 초기 학습 문서에 남긴 수치와 당시 목표를 보존한 표다. Phase 1·2 수치의 실행 시점·동일 조건 여부·원시 측정 자료를 이 표에서 확인할 수 없으므로 검증된 비교 결과로 사용하지 않는다. Phase 3의 지연·RPS는 측정값이 아닌 목표다. 이후 [2026-09-12의 HTTP 응답 비교](benchmark-2026-09-12.md) 및 [2026-10-22의 독립 실험](experiments-2026-10-22.md)과 합쳐 해석하지 않는다. 아래 발급 수 행은 개수 수준의 기록이며 장애 내구성이나 최종 발급 지연을 입증하지 않는다.

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
Phase 3: Redis → Kafka 전송 요청 → 응답 → [나중에 MySQL]
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

가용성 복구와 데이터 무손실은 별개다. Redis 복제는 기본적으로 비동기이므로 failover 때 최근 재고·중복 정보가 유실될 수 있다. DB에 아직 쓰이지 않은 예약까지 고려해 복구 기준을 정해야 하며, DB 수량만 읽어 재고를 다시 채우면 진행 중 발급을 놓칠 수 있다. [Redis 복제와 failover 한계](https://redis.io/docs/latest/operate/oss_and_stack/management/replication/)

### 9-2. Kafka 브로커 단일 장애점

이 프로젝트는 단일 Kafka 브로커다.  
프로덕션에서는 Replication Factor=3으로 설정하고, 브로커 3대 이상 운영한다.

```yaml
KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: "3"  # 브로커 3대
KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: "3"
```

위 설정은 내부 토픽의 복제 설정이다. 발급 토픽 `coupon-issue` 자체의 replication factor, `min.insync.replicas`, Producer `acks=all`도 함께 정해야 한다. 현재 단일 브로커 환경으로는 브로커 장애 시 내구성을 검증할 수 없다. [Kafka 토픽 설정](https://kafka.apache.org/37/generated/topic_config.html)

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
   핵심 아이디어: "DB Lock 대신 Redis 개별 명령의 원자성 활용"
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
  │  Step 1. Redis에서 쿠폰 기간 검사          │
  │  Step 2. Redis 개별 명령 호출              │
  │    ├─ SADD coupon:{id}:users {userId}       │
  │    │    → 0이면 중복 → 예외                 │
  │    └─ DECR coupon:{id}:stock               │
  │         → 음수면 INCR + SREM 후 예외      │
  │  Step 3. Kafka 전송 요청 (완료 대기 없음)  │
  └──────┬───────────────────┬─────────────────┘
         │                   │
         ▼                   ▼
  ┌─────────────┐   ┌─────────────────────┐
  │    Redis    │   │        Kafka        │
  │             │   │  topic: coupon-issue│
  │ stock: 999  │   │  key: {couponId}    │
  │ users: Set  │   │  (키별 파티션 배치) │
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

- Redis: `SADD`와 `DECR`은 각각 원자적이다. 두 호출 전체와 실패 보상은 하나의 원자적 작업이 아니다.
- Kafka: DB 저장을 비동기로 분리한다. 현재 API는 Kafka 전송 완료도 기다리지 않는다.
- `202 Accepted`: 처리 요청을 접수했다는 응답이며, Kafka 기록 성공이나 최종 발급을 보장하지 않는다.
- 정합성: `report.sh`의 수량 대조는 점검 수단이다. 장애 시에는 요청 ID와 사용자별 발급 이력까지 대조해야 누락·중복을 찾을 수 있다.
