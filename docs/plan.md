# 선착순 쿠폰 발급 시스템 — 학습 & 구현 계획

> 전략: **계층적 구현 (Incremental Phase)**
> 단순한 방법부터 동작시키고, 문제를 직접 경험하면서 고도화한다.
> 각 Phase는 독립적으로 동작 가능한 결과물을 만든다.

---

## 전체 로드맵

```
Phase 0 → Phase 1 → Phase 2 → Phase 3 → Phase 4
환경 세팅    DB Lock    Redis     Kafka     Read Replica
(1~2일)    (2~3일)   (3~4일)   (3~4일)    (2~3일)
```

---

## Phase 0 — 환경 세팅 + 사전 개념

**목표:** 코드 한 줄 없이도 3개 인프라가 로컬에서 실행된다.

### 공부할 것

#### Docker / Docker Compose
- **Image vs Container 차이** — Image는 설계도, Container는 실행 인스턴스
- **Volume** — 컨테이너가 삭제돼도 데이터가 남는 이유
- **Network** — 같은 compose 파일 안의 서비스끼리 서비스명으로 통신하는 원리
- `docker-compose up -d`, `docker-compose logs -f`, `docker exec -it` 명령어

#### JPA 기본
- `@Entity`, `@Table`, `@Column` 어노테이션
- `@OneToMany`, `@ManyToOne` 연관관계
- `@Transactional` 동작 원리 (프록시 기반 AOP)
- 영속성 컨텍스트 (Persistence Context) — 1차 캐시, 변경 감지(Dirty Checking)

#### Spring Boot 프로젝트 구조
- Controller → Service → Repository 계층 분리 이유
- `@RestController`, `@Service`, `@Repository` 차이
- `application.properties` 설정 방식

### 구현할 것

#### docker-compose.yml 완성
현재 파일에 Kafka KRaft 설정이 빠져 있다. 아래 항목을 추가한다:

```
- MySQL 8.0 (포트 3306, DB명: coupon_db, utf8mb4)
- Redis 7 Alpine (포트 6379)
- Kafka KRaft 모드 (포트 9092, Zookeeper 없이)
  - KAFKA_PROCESS_ROLES=broker,controller
  - KAFKA_NODE_ID=1
  - KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093
```

#### 도메인 엔티티 작성
- `Coupon.java` — ASSIGNMENT.md 4절 참조
- `IssuedCoupon.java`
- `CouponUsageHistory.java`
- Enum: `CouponType`, `CouponStatus`, `IssuedCouponStatus`

#### build.gradle 의존성 추가
- `spring-boot-starter-data-redis` (이미 있음, 확인)
- `spring-kafka` (누락됨, 추가 필요)

### 체크포인트
- [x] `docker-compose up -d` 실행 후 3개 서비스 모두 `healthy` ✅ 2026-05-23
- [x] `docker exec -it mysql mysql -u root -p` 접속 확인 ✅ 2026-05-23
- [x] `docker exec -it redis redis-cli ping` → `PONG` ✅ 2026-05-23
- [x] 애플리케이션 실행 시 JPA가 테이블 자동 생성 ✅ 2026-05-23

---

## Phase 1 — DB Lock으로 동시성 제어

**목표:** Redis/Kafka 없이 DB만으로 동시성 문제를 해결한다. 이 단계에서 동시성의 어려움을 직접 체감한다.

### 공부할 것

#### 트랜잭션 격리 수준 (Transaction Isolation Level)
4가지 격리 수준을 이해하고 MySQL 기본값이 왜 REPEATABLE READ인지 파악한다:

| 레벨 | Dirty Read | Non-Repeatable Read | Phantom Read |
|------|-----------|---------------------|--------------|
| READ UNCOMMITTED | 발생 | 발생 | 발생 |
| READ COMMITTED | 방지 | 발생 | 발생 |
| **REPEATABLE READ** (MySQL 기본) | 방지 | 방지 | 방지* |
| SERIALIZABLE | 방지 | 방지 | 방지 |

#### Pessimistic Lock (비관적 락)
- `SELECT ... FOR UPDATE` — Row를 잠금, 다른 트랜잭션이 같은 Row를 읽으려 하면 대기
- JPA에서 사용법: `@Lock(LockModeType.PESSIMISTIC_WRITE)`
- 언제 쓰나: 충돌이 자주 발생하는 경우 (선착순처럼 경쟁이 심한 상황)
- 단점: 대기 시간이 생기고 커넥션 풀이 빨리 고갈될 수 있음

#### Optimistic Lock (낙관적 락)
- `@Version` 컬럼을 두고, 업데이트 시 버전이 맞지 않으면 `OptimisticLockException` 발생
- 언제 쓰나: 충돌이 드문 경우
- 이 과제에는 부적합 — 선착순이라 충돌이 매우 잦기 때문. 재시도 로직이 복잡해짐

#### 데드락 (Deadlock)
- A가 Row1을 잠그고 Row2를 기다리는데, B가 Row2를 잠그고 Row1을 기다릴 때 발생
- MySQL은 데드락 감지 후 한쪽 트랜잭션을 롤백
- Lock 획득 순서를 일관되게 유지하면 예방 가능

#### 동시성 테스트 방법
- `ExecutorService` + `CountDownLatch` 패턴:
  ```java
  // 모든 스레드가 동시에 출발하도록 CountDownLatch(1)로 대기
  // 완료를 기다리기 위해 CountDownLatch(N) 사용
  ```
- `AtomicInteger`로 성공/실패 카운트

### 구현할 것

#### API 6개 완성
```
GET  /api/coupons/available          → 발급 가능한 쿠폰 목록
POST /api/coupons/{couponId}/issue   → 선착순 발급 (핵심)
GET  /api/coupons/{couponId}/stock   → 잔여 수량 조회
GET  /api/coupons/users/{userId}     → 사용자 보유 쿠폰
GET  /api/coupons/users/{userId}/usable → 사용 가능한 쿠폰
POST /api/coupons/{issuedCouponId}/use  → 쿠폰 사용
```

#### 발급 로직 (Pessimistic Lock 적용)
```
1. couponId로 Coupon 조회 (FOR UPDATE로 잠금)
2. 발급 기간 체크 → 아니면 CouponNotAvailableException
3. 재고 체크 → 없으면 CouponExhaustedException
4. userId로 중복 발급 체크 → 있으면 DuplicateCouponIssueException
5. issuedQuantity + 1, IssuedCoupon 생성
6. 저장 후 반환
```

#### GlobalExceptionHandler
```java
@RestControllerAdvice
public class ApiControllerGlobalAdvice {
    @ExceptionHandler(CouponException.class)
    public ResponseEntity<ApiResponse<?>> handleCouponException(CouponException e) { ... }
}
```

#### 동시성 테스트 2개
- 시나리오 1: 스레드 1000개, 쿠폰 100개 → 성공 100개 확인
- 시나리오 2: 스레드 100개, 동일 userId → 성공 1개 확인

### 체크포인트
- [x] 모든 API가 정상 응답 ✅ 2026-05-23
- [x] 동시성 테스트 시나리오 1, 2 모두 통과 ✅ 2026-05-23
- [x] 예외 발생 시 공통 응답 포맷(`success: false, message: ...`) 반환 ✅ 2026-05-23

---

## Phase 2 — Redis로 성능 + 분산 락

**목표:** DB Lock의 병목을 Redis로 해소한다. 응답 속도가 Phase 1 대비 눈에 띄게 빨라진다.

### 공부할 것

#### Redis 자료구조 (이 과제에서 쓰는 것 위주)
| 자료구조 | 명령어 | 이 과제에서 용도 |
|---------|-------|---------------|
| String | `GET`, `SET`, `INCR`, `DECR` | 쿠폰 잔여 수량 카운터 |
| Set | `SADD`, `SISMEMBER` | 중복 발급 체크 (userId 집합) |
| String | `SET key value NX PX` | 분산 락 |

#### INCR의 원자성이 왜 중요한가
- Redis는 Single-threaded — 명령어가 하나씩 순서대로 처리됨
- `INCR`은 Read-Modify-Write가 한 번에 → 경쟁 조건(Race Condition) 없음
- DB의 `UPDATE SET count = count + 1`은 트랜잭션 없으면 안전하지 않음

#### 분산 락 (Distributed Lock)
- **왜 필요한가**: 서버가 여러 대일 때 JVM 내부 `synchronized`는 의미 없음
- **SETNX 방식** (`SET key value NX PX 3000`):
  - `NX` — 키가 없을 때만 SET (락 획득)
  - `PX 3000` — 3초 후 자동 만료 (데드락 방지)
  - 문제: 락 만료 후 다른 스레드가 내 락을 해제할 수 있음
- **Redlock 알고리즘**: Redis 노드 N개 중 과반수에 락을 획득해야 유효
- **Redisson**: Redlock을 Java로 구현한 라이브러리. 락 갱신(Watchdog), 재진입 등 처리

#### Cache-aside 패턴 (Look-aside)
```
1. Cache에서 데이터 조회
2. 있으면(Cache Hit) → 바로 반환
3. 없으면(Cache Miss) → DB 조회 → Cache에 저장 → 반환
```
- 잔여 수량 조회 API에 적용: 매번 DB 조회 대신 Redis 캐시 활용

#### Cache Stampede
- 인기 캐시가 만료되는 순간 수백 개의 요청이 동시에 DB 조회 → DB 과부하
- 해결법: TTL에 랜덤 지터 추가, Mutex 락, Background Refresh

#### CQRS 기초 개념
- Command (쓰기): 상태를 변경 → 정합성이 중요 → DB Primary
- Query (읽기): 상태를 조회 → 빠른 응답이 중요 → Redis 캐시 또는 Replica
- 이 과제에서 적용: 발급(Command)은 DB, 잔여 수량 조회(Query)는 Redis

### 구현할 것

#### Redisson 의존성 추가
```groovy
implementation 'org.redisson:redisson-spring-boot-starter:{최신버전}'  // MVN Repository에서 최신 버전 확인
```

#### Redis로 수량 제어 + 중복 방지
```
발급 요청 흐름:
1. SISMEMBER coupon:{couponId}:issued {userId} → true면 중복 예외
2. INCR coupon:{couponId}:count → 반환값이 totalQuantity 초과면 DECR 후 소진 예외
3. SADD coupon:{couponId}:issued {userId}
4. DB에 IssuedCoupon 저장
```

#### Redisson 분산 락 적용
```java
RLock lock = redissonClient.getLock("coupon:lock:" + couponId);
try {
    lock.tryLock(3, 1, TimeUnit.SECONDS); // 최대 3초 대기, 1초 후 만료
    // 발급 로직
} finally {
    lock.unlock();
}
```

#### 잔여 수량 조회 캐시
- `GET coupon:{couponId}:stock` → 없으면 DB 조회 후 캐시 (TTL 10초)

#### 성능 비교
- Phase 1과 Phase 2를 Apache Bench나 k6로 비교
  ```bash
  ab -n 1000 -c 100 -p body.json -T application/json \
     http://localhost:8080/api/coupons/1/issue
  ```

### 체크포인트
- [ ] DB Lock 없이도 동시성 테스트 통과
- [ ] 동일 userId 중복 발급 방지 (Redis Set으로)
- [ ] 잔여 수량 조회 응답이 캐시 히트 시 빠름
- [ ] Phase 1 대비 TPS(초당 처리량) 향상 확인

---

## Phase 3 — Kafka 비동기 처리 (쓰기 분산)

**목표:** 발급 요청을 즉시 202로 응답하고, 실제 처리는 비동기로. 피크 트래픽을 흡수하는 구조를 만든다.

### 공부할 것

#### Kafka 핵심 개념

**Topic / Partition / Offset**
- Topic: 메시지 분류 이름 (이 과제: `coupon-issue-request`)
- Partition: Topic을 나눈 물리적 단위. 병렬 처리를 위해 존재
- Offset: Partition 안에서 메시지의 순서 번호. Consumer가 어디까지 읽었는지 기록
- 메시지는 Partition 안에서만 순서가 보장됨

**Producer / Consumer / Consumer Group**
- Producer: 메시지를 Topic에 publish
- Consumer: Topic에서 메시지를 subscribe
- Consumer Group: 같은 Group의 Consumer들이 Partition을 나눠서 처리 (병렬 소비)
  - Consumer Group 내에서 Partition 1개는 Consumer 1개만 담당

**KRaft 모드 (Zookeeper 없는 이유)**
- 기존: Kafka가 메타데이터(브로커 목록, 토픽 정보)를 Zookeeper에 저장
- KRaft: Kafka 자체가 Raft 합의 알고리즘으로 메타데이터 관리
- 장점: 운영 복잡도 감소, 더 빠른 리더 선출

#### 메시지 전달 보장 수준
| 수준 | 설명 | 이 과제 적용 |
|------|------|------------|
| At-most-once | 최대 1번 (유실 가능) | 사용 안 함 |
| **At-least-once** | 최소 1번 (중복 가능) | Consumer 실패 시 재처리 |
| Exactly-once | 정확히 1번 | 트랜잭션 설정 필요, 복잡 |

At-least-once를 쓰면 Consumer가 중복 처리할 수 있음 → Redis의 SADD로 중복 방지가 이미 있어서 OK.

#### DLQ (Dead Letter Queue)
- Consumer가 메시지 처리에 계속 실패하면 무한 재시도 → 다른 메시지가 막힘
- 일정 횟수 실패 후 별도 Topic(`coupon-issue-dlq`)으로 이동
- 운영자가 DLQ를 모니터링하고 수동 재처리

#### 쓰기 분산 패턴
```
[Before] 요청 → 서버 → DB (동기, 느림, DB 부하)
[After]  요청 → 서버 → Kafka (즉시 202 응답)
                        ↓
                   Consumer → DB (비동기, DB 부하 평준화)
```
- 피크 트래픽이 와도 Kafka가 버퍼 역할 → 서버/DB가 처리 가능한 속도로 소비

### 구현할 것

#### 의존성 추가
```groovy
implementation 'org.springframework.kafka:spring-kafka'
```

#### Topic 설계
```
coupon-issue-request  — 발급 요청 메시지
coupon-issue-dlq      — 실패한 메시지
```

#### Producer (CouponController)
```java
// POST /api/coupons/{couponId}/issue
public ResponseEntity<ApiResponse<?>> issueCoupon(...) {
    // Redis로 즉시 수량/중복 검증
    // Kafka에 메시지 publish
    return ResponseEntity.accepted()
        .body(ApiResponse.success("쿠폰 발급 요청이 접수되었습니다."));
}
```

#### Consumer (CouponIssueConsumer)
```java
@KafkaListener(topics = "coupon-issue-request", groupId = "coupon-group")
public void consume(CouponIssueMessage message) {
    // DB에 IssuedCoupon 저장
    // 실패 시 DLQ로 이동
}
```

#### 메시지 객체 설계
```java
public record CouponIssueMessage(
    Long couponId,
    Long userId,
    LocalDateTime requestedAt
) {}
```

#### DLQ 처리
- `@RetryableTopic(attempts = 3, dltTopicSuffix = "-dlq")`
- 또는 `DeadLetterPublishingRecoverer` 직접 구성

### 체크포인트
- [ ] 발급 요청 시 즉시 202 응답
- [ ] Kafka Consumer 로그에서 실제 발급 처리 확인
- [ ] Consumer를 강제 종료해도 재시작 후 미처리 메시지 재소비
- [ ] 처리 불가 메시지가 DLQ topic으로 이동

---

## Phase 4 — MySQL Read Replica (읽기/쓰기 분산)

**목표:** 조회 트래픽을 Replica로 분산해 Primary DB 부하를 줄인다. CQRS를 인프라 레벨까지 구현한다.

### 공부할 것

#### MySQL Replication 원리
- **Primary (Master)**: 쓰기 처리. 모든 변경사항을 Binary Log(binlog)에 기록
- **Replica (Slave)**: Primary의 binlog를 읽어 똑같이 재실행 (비동기 복제)
- **복제 지연 (Replication Lag)**: Replica가 Primary보다 약간 뒤처질 수 있음
  - 발급 직후 잔여 수량 조회 시 이전 값이 나올 수 있음 → 수용 가능한지 판단 필요

#### Spring에서 읽기/쓰기 DataSource 분리

**AbstractRoutingDataSource**
- 런타임에 어떤 DataSource를 쓸지 결정하는 Spring 추상 클래스
- `determineCurrentLookupKey()` 오버라이드 → `"primary"` 또는 `"replica"` 반환

**라우팅 기준**
```
@Transactional(readOnly = true) → Replica DataSource
@Transactional                  → Primary DataSource
```

**AOP로 자동 감지**
```java
@Aspect
public class ReadOnlyRoutingAspect {
    @Around("@annotation(transactional)")
    public Object route(ProceedingJoinPoint pjp, Transactional transactional) {
        if (transactional.readOnly()) {
            DataSourceContextHolder.set("replica");
        }
        // ...
    }
}
```

#### CQRS 심화
- Phase 2에서 "쓰기는 DB, 읽기는 Redis 캐시"로 논리적 CQRS 적용
- Phase 4에서 "쓰기는 Primary, 읽기는 Replica"로 인프라 레벨 CQRS 적용
- 두 레이어를 조합하면:
  ```
  잔여 수량 조회 → Redis 캐시 히트 → 반환 (DB 안 탐)
               → 캐시 미스 → Replica DB 조회 → 캐시 저장 → 반환
  쿠폰 발급     → Primary DB 직접 처리
  ```

#### 복제 지연 대응 전략
- 발급 직후 조회가 필요한 경우 Primary 강제 라우팅 (`@Transactional` 사용)
- 또는 Redis 캐시를 신뢰하고 Replica 결과 차이를 허용

### 구현할 것

#### docker-compose에 MySQL Replica 추가
```yaml
mysql-replica:
  image: mysql:8.0
  environment:
    MYSQL_ROOT_PASSWORD: root
    MYSQL_DATABASE: coupon_db
  volumes:
    - mysql-replica-data:/var/lib/mysql
    - ./mysql/replica.cnf:/etc/mysql/conf.d/replica.cnf
  depends_on:
    - mysql
```

#### Primary/Replica my.cnf 설정
```ini
# primary.cnf
[mysqld]
server-id = 1
log-bin = mysql-bin
binlog-format = ROW

# replica.cnf
[mysqld]
server-id = 2
relay-log = relay-bin
read-only = ON
```

#### RoutingDataSource 구현
```java
public class RoutingDataSource extends AbstractRoutingDataSource {
    @Override
    protected Object determineCurrentLookupKey() {
        return DataSourceContextHolder.get(); // "primary" or "replica"
    }
}
```

#### 서비스 어노테이션 적용
```java
// 조회 → Replica
@Transactional(readOnly = true)
public List<CouponResponse> getAvailableCoupons() { ... }

// 발급 → Primary
@Transactional
public void issueCoupon(Long couponId, Long userId) { ... }
```

### 체크포인트
- [ ] MySQL Replica가 Primary 데이터를 실시간으로 복제
- [ ] `SHOW REPLICA STATUS` 에서 `Seconds_Behind_Source: 0` 확인 (MySQL 8.0+)
- [ ] MySQL general_log 또는 Replica 쿼리 로그로 읽기 분리 확인
- [ ] 동시성 테스트 통과 (쓰기는 Primary, 읽기는 Replica)

---

## 학습 참고 순서 요약

| Phase | 핵심 키워드 (이것부터 검색) |
|-------|--------------------------|
| 0 | `Docker Compose volume network`, `JPA 영속성 컨텍스트` |
| 1 | `MySQL 트랜잭션 격리 수준`, `JPA Pessimistic Lock SELECT FOR UPDATE` |
| 2 | `Redis SETNX 원자성`, `Redisson 분산 락 Watchdog`, `Cache-aside 패턴` |
| 3 | `Kafka Consumer Group Offset`, `KRaft 모드`, `At-least-once DLQ` |
| 4 | `MySQL Replication binlog`, `AbstractRoutingDataSource`, `CQRS 읽기쓰기 분리` |

---

## 동시성 테스트 코드 뼈대

```java
@Test
void 천명_동시_발급_정확히_100개만_성공() throws InterruptedException {
    int threadCount = 1000;
    int couponQuantity = 100;
    ExecutorService executor = Executors.newFixedThreadPool(32);
    CountDownLatch startLatch = new CountDownLatch(1);  // 동시 출발
    CountDownLatch doneLatch = new CountDownLatch(threadCount);  // 완료 대기
    AtomicInteger successCount = new AtomicInteger();

    for (int i = 0; i < threadCount; i++) {
        long userId = i;
        executor.submit(() -> {
            try {
                startLatch.await();  // 모두 대기
                couponService.issueCoupon(COUPON_ID, userId);
                successCount.incrementAndGet();
            } catch (Exception ignored) {
            } finally {
                doneLatch.countDown();
            }
        });
    }

    startLatch.countDown();  // 동시 출발
    doneLatch.await();

    assertThat(successCount.get()).isEqualTo(couponQuantity);
}
```

---

## 예상 총 소요 기간

| Phase | 예상 기간 |
|-------|---------|
| Phase 0 | 1~2일 |
| Phase 1 | 2~3일 |
| Phase 2 | 3~4일 |
| Phase 3 | 3~4일 |
| Phase 4 | 2~3일 |
| **합계** | **11~16일** |

> 하루 3~4시간 기준. 개념이 처음이라면 각 Phase에 하루 더 여유를 두는 것을 추천.
