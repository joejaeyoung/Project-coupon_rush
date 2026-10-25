# Phase 2. Redis로 동시성 제어
## 선착순 쿠폰 시스템 구축기 (3) — 오버셀링 아키텍처 진화

> 2026-10-22 보완: 과거 실험·학습 기록을 유지하되 현재 구현과 대안을 구분했다. 현재 Phase 2/3는 Lua·Redisson 없이 `SADD → DECR`를 개별 호출한다. 동시성·장애 정합성·확장성의 추가 질문은 [멘토링 논의안](mentoring-discussion.md)을 참고한다.

> 후속 실증: 별도 실험 모듈(2026-10-26 main에서 제거, 코드는 [커밋 dc0e901](https://github.com/joejaeyoung/Project-coupon_rush/tree/dc0e901/4-coupon_experiments))에 Lua 예약과 Redis Stream 전달을 구현했다. 분리 명령의 중간 실패, Lua 런타임 오류의 부분 쓰기, 복제 지연 후 승격, Cluster 키 편중 결과는 [실증 보고서](experiments-2026-10-22.md)에 기록한다. 본문의 기존 Phase 2 구현과 구분한다. 현재 4단계 [4-coupon_redisHA_kafka](../4-coupon_redisHA_kafka/README.md)는 Lua·Stream 없이 Phase 3에 Kafka ACK 확인과 보상, DB 유니크 제약을 더한 최소 구성이다. Redis는 Sentinel HA 구성이며 예약은 replica 수신(`WAIT`)을 확인한 뒤 승인한다.

> Phase 1에서 Pessimistic Lock으로 정확성은 잡았다.  
> 근데 p95가 2.32초, RPS 409. 이걸 Redis로 바꾸면 얼마나 달라질까?  
> 그 전에, 왜 DB Lock이 한계에 부딪히는지 실제 서비스 관점에서 먼저 이해해야 한다.

---

## 1. 재고 관리가 왜 어려운가

### 단일 Row에 집중되는 트래픽

쿠폰 100개를 사람 10만 명이 동시에 요청하면 어떤 일이 생기나.

```
쿠폰 테이블의 id=1 Row
                  ↑ ↑ ↑ ↑ ↑ ↑ ↑ ↑ ... (10만 개의 동시 요청)
```

애플리케이션 Scale-out(서버 증설)만으로 같은 DB Row의 직렬화가 없어지지는 않는다.
**트래픽이 분산되지 않고 단 하나의 DB Row에 꽂히기 때문이다.**  
서버가 100대여도 결국 같은 Row에 Lock을 걸려고 줄을 선다.

쿠폰 ID로 샤딩하면 여러 쿠폰은 분산할 수 있지만 한 쿠폰의 부하는 한곳에 남는다. 재고를 여러 버킷으로 쪼개는 대안도 있으나 총수량 보장, 남은 수량 재배분, 선착순 기준을 다시 설계해야 한다. 서버 증설로 해결되는 범위와 이런 데이터 모델 변경을 구분한다.

### 0.1초의 전쟁

선착순 이벤트의 특성상 트래픽은 **순간적으로 폭주**한다.  
오전 10시 정각, 모두가 F5를 누른다. Auto Scaling이 완료되기 전에 이미 인프라가 마비된다.  
Scale-out은 준비 시간이 필요하지만 트래픽은 기다려주지 않는다.

### 재고는 언제 차감해야 하는가

쇼핑몰의 결제 흐름을 보면 재고 차감 시점을 어디에 넣을지 명확하지 않다.

```
[카탈로그 조회] → [상품 상세] → [장바구니] → [결제 시작] → [PG 결제창] → [결제 완료] → [주문 완료]
      ↑                                           ↑              ↑ 여기서 우리 서버 모름
   재고 표시                                  재고 잠금?       타임아웃 처리
```

PG(결제대행사) 결제창은 **PG 서버에서 렌더링**된다. 사용자가 결제창에 있는 동안 우리 서버는 상태를 알 수 없다. 20분이 넘어도 모른다.

각 시점마다 문제가 있다:

| 차감 시점 | 문제 |
|----------|------|
| 장바구니 담기 | 악성 재고 점유 (담아만 놓고 안 삼) |
| 결제 시작 | 이탈 유저 발생 시 재고 복구 필요 → 복구 안 되면 유령 재고 |
| PG 결제창 | 우리 서버가 상태 모름, 타임아웃 처리 필수 |
| 결제 완료 후 | 카드는 긁혔는데 재고 없으면 치명적 오버셀링 |

**정답은 없다.** 비즈니스 정책과 트레이드오프다. 다만 결제 완료 후 차감이 가장 위험하다.

---

## 2. 오버셀링(Overselling)이 생기는 정확한 이유

Phase 1의 락 없는 버전 코드로 돌아가보자.

```java
// ❌ 락 없는 버전
Coupon coupon = couponRepository.findById(couponId);      // SELECT
if (coupon.getIssuedQuantity() < coupon.getTotalQuantity()) {
    coupon.setIssuedQuantity(coupon.getIssuedQuantity() + 1); // 메모리에서 수정
    // @Transactional이 커밋할 때 UPDATE
}
```

MySQL의 기본 격리 수준은 **REPEATABLE READ**다.  
트랜잭션 시작 시점의 **스냅샷**을 읽는다.

```
T=0: Thread A 트랜잭션 시작 → stock 스냅샷: 1
T=0: Thread B 트랜잭션 시작 → stock 스냅샷: 1
T=1: Thread A: 스냅샷 읽음 → stock=1, 발급 가능 → UPDATE stock=0
T=2: Thread B: 스냅샷 읽음 → stock=1, 발급 가능 → UPDATE stock=0  ← 0 덮어씀!
```

0.1초만 트랜잭션이 길어져도 **과거 데이터를 읽고 덮어쓴다.**

`UPDATE stock SET stock = stock - 1 WHERE id = 1 AND stock > 0` 이렇게 하면 안 되냐?  
→ 이건 단순 재고 감소엔 유효하지만, **중복 발급 체크 + 재고 감소를 동시에** 원자적으로 처리하기 어렵다.

결국 Pessimistic Lock(`SELECT FOR UPDATE`)으로 해결했지만:

```
DB 커넥션 풀: 기본 10개

1000명 동시 요청
→ 10개는 Lock 획득 후 처리
→ 990개는 Lock 대기 + 커넥션 점유
→ 커넥션 풀 고갈 → 503 에러
```

Read가 많이 발생하는 환경에서 Row-Level Lock은 위험하다.  
1초에 Lock 처리 속도 vs 새로운 요청이 들어오는 속도를 비교하면 1000배 차이가 날 수 있다.

---

## 3. Redis가 이걸 어떻게 해결하는가

### RDBMS의 한계 → Redis

핵심은 **Read Operation을 DB에서 없애는 것**이다.

```
Before (Phase 1):
클라이언트 → Spring → MySQL (Lock → Read → Write)
                              ↑ 여기가 병목

After (Phase 2):
클라이언트 → Spring → Redis (원자적 DECR)
                    ↓ (성공 시에만)
                    MySQL (Write-through or 비동기)
```

Redis가 빠른 이유 3가지:
1. **메모리 기반** — 디스크 I/O 없음, 응답이 μs 단위
2. **싱글 스레드** — Race Condition 자체가 없음, Lock 필요 없음
3. **O(1) 자료구조** — 해시 테이블 기반, 데이터 양에 무관하게 일정한 속도

> Redis 기본 개념, 자료구조, CLI 명령어 → `redis_study.md` 참고

### Master-Slave 구조로 부족한 이유

Read가 많으면 Master-Slave(Read Replica)로 읽기를 분산하면 안 되나?

카카오 내비 같은 경우 Read:Write = 1000만:1만이면 Slave로 분산이 효과적이다.  
하지만 선착순 이벤트처럼 **Write가 순간적으로 폭발**하면:

```
Write 10만 건 발생
→ Master가 뻗음
→ Master → Slave 동기화 폭발
→ Read Replica도 동기화 부하로 뻗음
→ 동기화 지연 → Slave가 과거 데이터 반환
```

Write 부하를 Redis에서 흡수하고, DB에는 최종 결과만 쓰는 구조가 필요하다.

---

## 4. Phase 2 구현 — Redis로 재고 제어

### 4-1. INCR/DECR: 원자적 수량 제어

```bash
# 쿠폰 1번 재고 초기화 (서버 시작 시 or 쿠폰 생성 시)
SET coupon:1:stock 100

# 발급 요청 — 원자적으로 카운터 증가 후 확인
INCR coupon:1:issued_count  → 1  (첫 번째 발급)
INCR coupon:1:issued_count  → 2  (두 번째 발급)
...
INCR coupon:1:issued_count  → 101 → 101 > 100 → 실패, DECR로 되돌리기
```

`INCR`은 Read-Modify-Write를 **단 하나의 명령어**로 처리한다.  
싱글 스레드이므로 1만 명이 동시에 `INCR`해도 하나씩 순서대로 실행된다. Race Condition 없음.

애플리케이션 레벨에서 `GET`으로 읽고 `SET`으로 쓰면 여전히 Race Condition이 생긴다.  
반드시 `INCR`/`DECR` 같은 **Redis 자체 명령어의 원자성**을 활용해야 한다.

### 4-2. SADD: 중복 발급 방지

```bash
# 유저 9999가 쿠폰 1번 발급 시도
SADD coupon:1:users 9999  → 1 (추가됨, 처음 발급)
SADD coupon:1:users 9999  → 0 (이미 있음, 중복!)
```

`SADD`는 집합에 원소를 추가하되, **이미 있으면 0을 반환**한다.  
반환값 하나로 "처음인지 중복인지" 판단할 수 있다.

### 4-3. Lua Script: 여러 연산을 하나의 트랜잭션처럼

재고 감소 + 중복 체크를 각각 별도 명령어로 날리면 두 명령어 사이에 개입이 가능하다.  
Lua Script로 묶으면 **네트워크 왕복 없이** Redis 단일 스레드 내에서 하나의 연산처럼 실행된다.

```lua
-- coupon_issue.lua
local couponKey = KEYS[1]   -- "coupon:1:issued_count"
local usersKey  = KEYS[2]   -- "coupon:1:users"
local userId    = ARGV[1]   -- "9999"
local maxCount  = tonumber(ARGV[2])  -- 100

-- 중복 체크 (SADD 반환 0이면 이미 발급)
local added = redis.call('SADD', usersKey, userId)
if added == 0 then
    return -1  -- 중복
end

-- 재고 차감
local count = redis.call('INCR', couponKey)
if count > maxCount then
    redis.call('DECR', couponKey)
    redis.call('SREM', usersKey, userId)
    return -2  -- 재고 소진
end

return count  -- 성공, 몇 번째 발급인지 반환
```

위 Lua 코드는 발급 수를 `INCR`하는 학습용 예제이며 현재 구현이 아니다. 현재 Phase 2/3는 남은 재고를 `DECR`하고 `SADD`로 중복을 판정한다. 각 명령의 원자성과 예약 전체의 원자성은 다르므로, 명령 사이 프로세스 종료·네트워크 실패까지 고려하면 중복 확인·재고 확인·차감·사용자 등록을 Lua 등으로 묶는 개선을 검토해야 한다. Lua도 Redis와 DB/Kafka 사이의 트랜잭션을 대신하지 않는다. [Redis Lua 문서](https://redis.io/docs/latest/develop/programmability/eval-intro/)

### 4-4. Redisson 분산 락: 복잡한 비즈니스 로직 보호

서버가 30대라면 각 서버가 독립적으로 Redis에 접근한다.  
`INCR`/`SADD` 원자성만으로 해결되지 않는 경우가 있다:

- 쿠폰 적용 (5만원 이상 시 1만원 할인) — 여러 조건 체크
- 사용자당 1개 구매 제한 — 장바구니 수량 확인
- 결제 세션 관리 — 여러 단계의 상태 전환

이런 **여러 단계의 연산을 하나의 임계구역**으로 묶으려면 분산 락이 필요하다.

```
실서비스 흐름 (Redisson 분산 락):

1. 분산 락 획득 (락 없으면 대기)
2. Redis Lua Script로 재고 차감
3. Delayed MQ에 "재고 선점" 메시지 발행 (5분 타임아웃)
4. 결제창으로 넘김
5. 분산 락 해제
```

```java
RLock lock = redissonClient.getLock("coupon:lock:" + couponId);
try {
    boolean acquired = lock.tryLock(3, 1, TimeUnit.SECONDS);
    if (!acquired) throw new CouponException(CouponErrorCode.EXHAUSTED);
    
    // 임계 구역
    redisTemplate.execute(luaScript, keys, userId, maxCount);
} finally {
    if (lock.isHeldByCurrentThread()) lock.unlock();
}
```

**Lettuce(Spin Lock) vs Redisson(Pub/Sub)**:
- Lettuce: 락을 얻을 때까지 반복적으로 `SETNX` 재시도 → Redis에 불필요한 부하
- Redisson: Pub/Sub으로 락 해제 이벤트를 구독 → 해제 시 알림 받아서 대기, CPU 낭비 없음

현재 Phase 2/3는 `SADD → DECR`와 실패 시 `INCR → SREM` 보상으로 구현되어 있다. Lua·Redisson은 도입한 기능이 아니라 비교할 대안이다. 정상 동시 요청 테스트만으로 명령 사이 장애나 보상 실패 때의 정합성까지 확인했다고 볼 수 없다.

---

## 5. 재고 복구 문제 — 유령 재고

결제 창에 진입했다가 이탈한 사용자의 "선점된 재고"는 어떻게 복구하나.

**Redis TTL 만료 알림만으로 복구를 완료할 수 없는 이유:**
```
SET coupon:1:lock:9999 "reserved" EX 300  ← 5분 TTL

문제:
1. 키의 만료 시각과 만료 알림의 전달 시각은 다름
   → TTL을 설정했다고 그 시각에 재고 복구 작업까지 완료되는 것은 아님

2. 발송 보장 없음 (ACK 없음)
   → 만료 이벤트를 구독하다가 처리 실패해도 재시도 불가
   → 재고가 복구 안 되고 유실됨

3. 복구 작업 자체도 실패할 수 있음
   → 예약 상태와 만료 시각을 기록하고 재시도·중복 복구 방지를 설계해야 함
```

만료 이벤트는 지연될 수 있고 Redis Pub/Sub은 연결이 끊긴 구독자에게 알림을 재전달하지 않는다. 이는 TTL을 예약 기한으로 쓰지 못한다는 뜻이 아니라, 알림만을 복구의 유일한 근거로 삼지 말아야 한다는 뜻이다. [Redis 키 공간 알림](https://redis.io/docs/latest/develop/pubsub/keyspace-notifications/)

**Delayed MQ로 해결:**
```
재고 선점 시 → Delayed MQ에 "5분 후 복구" 메시지 발행
                              ↓ (5분 뒤)
              앱이 구독 중 → "결제 완료됐나 DB 확인"
              완료됨 → 아무것도 안 함
              미완료 → Redis INCR으로 재고 복구 + SREM으로 유저 제거
```

RabbitMQ도 여러 Consumer와 재전달 상황에서는 처리 순서를 별도로 고려해야 하며, ACK만으로 외부 DB 처리까지 원자적으로 보장되지는 않는다. Kafka와의 선택은 지연 전달, 재시도, 순서의 범위, 운영 방식으로 비교한다. [RabbitMQ 메시지 순서](https://www.rabbitmq.com/docs/queues#message-ordering)

이 프로젝트에는 결제 대기와 Delayed MQ가 구현되어 있지 않다. 위 흐름은 예약 만료가 필요한 서비스로 확장할 때의 비교 대상이다.

---

## 6. 실무 사례 — 피스마이너스원(GD), 웍스아웃

### 피스마이너스원 (G-Dragon 공식 사이트)

GD 컴백 한정판 상품. 트래픽이 예측 불가능하게 폭발한다.  
단일 상품에 트래픽이 집중되는 전형적인 선착순 재고 문제.

### 웍스아웃 (브랜드 쇼핑몰)

고도몰 기반으로 운영하다가 결제 TPS가 1밖에 나오지 않는 문제 발생.  
시스템을 처음부터 재설계했다.

적용된 기술:
- **Redisson이 아닌 Redis 직접 분산 락 구현** — 할인 로직이 복잡하게 엮여 있어 커스텀 락 필요
- **대기열 큐 (앞단 쓰로틀링)** — 뒷단(DB, Kafka)이 아닌 **앞단**에서 트래픽 제어
  - 수십만 명이 동시에 접근 → 파킹 페이지(대기 페이지) 표시
  - 내 차례가 되면 상품 페이지로 진입
  - 뒷단 인프라가 감당 가능한 속도로만 트래픽 유입

이게 핵심 전환점이다:  
**뒷단을 아무리 튜닝해도 앞단에서 걸러주지 않으면 한계가 있다.**

---

## 7. 아키텍처 진화 전체 그림

```
Phase 1: DB Lock
클라이언트 → Spring → MySQL (SELECT FOR UPDATE → UPDATE)
            ↑ 커넥션 풀 고갈, 응답 지연, TPS 제한

Phase 2: Redis 개별 원자적 연산
클라이언트 → Spring → Redis (SADD → DECR, 전체는 별도 호출)
                     → MySQL (Write-through)
            ↑ 판정에서 DB 락 제거, 승인된 요청의 DB 쓰기는 동기

Phase 3: Kafka 비동기 Write-Back
클라이언트 → Spring → Redis (즉시 응답)
                     → Kafka → MySQL (Throttling, DB 보호)
            ↑ 응답과 DB 저장 분리, 순서·실패 복구는 별도 설계 필요

추가 대안 예시 (현재 미구현, 모두 필요한 조합은 아님):
클라이언트 → 대기열 (앞단 쓰로틀링)
           → Spring → Redisson 락 → Redis Lua Script
                    → Delayed MQ (재고 선점 타임아웃)
                    → 결제창 (PG)
                    → Kafka → MySQL
```

---

## 8. Phase 2 구현 상세 — 코드 레벨

이 절의 코드는 설명을 위한 축약 예제다. 현재 구현은 [CouponRedisService](../2-coupon_redis/spring/src/main/java/com/example/coupon/service/CouponRedisService.java)와 [CouponService](../2-coupon_redis/spring/src/main/java/com/example/coupon/service/CouponService.java)를 기준으로 확인한다.

### 8-1. 전체 흐름 변화

Phase 1과 Phase 2의 `issueCoupon` 흐름을 나란히 비교하면:

```
Phase 1 (DB Lock):
POST /api/coupons/{couponId}/issue

  1. MySQL SELECT FOR UPDATE  ← DB 커넥션 점유 + 다른 요청 대기
  2. 기간 체크 (Java)
  3. 재고 체크 (Java)
  4. 중복 체크 (MySQL SELECT)
  5. issued_quantity + 1 (Dirty Checking → UPDATE)
  6. IssuedCoupon INSERT
  7. 트랜잭션 커밋 → DB Lock 해제

Phase 2 (Redis):
POST /api/coupons/{couponId}/issue

  1. 기간 체크 (Redis)
  2. 중복 체크 → Redis SADD  ← 0이면 중복 → 즉시 반환
  3. 재고 차감 → Redis DECR  ← 음수면 INCR + SREM → 재고 소진
  4. MySQL 수량 증가 + IssuedCoupon INSERT (TransactionTemplate)
  5. DB 실패 시 Redis 보상, 성공 시 완료
  ↑
  판정에서 DB 락을 제거한다. DB 쓰기는 Redis 판정을 통과한 요청만 수행한다.
```

### 8-2. Redis 초기화 — 쿠폰 재고를 언제 Redis에 올리나

쿠폰 발급 요청이 오기 전에 Redis에 재고를 세팅해야 한다.  
다음은 비교할 두 가지 방법이며 현재 그대로 구현된 흐름은 아니다:

**방법 A: 쿠폰 생성 시 Redis 초기화**
```java
// CouponService.createCoupon()
public void createCoupon(Coupon coupon) {
    couponRepository.save(coupon);
    // Redis에 재고 키 세팅
    redisTemplate.opsForValue().set(
        "coupon:" + coupon.getId() + ":stock",
        String.valueOf(coupon.getTotalQuantity())
    );
}
```

**방법 B: 발급 요청 시 Cache-aside**
```java
// 발급 요청이 왔을 때 Redis에 없으면 DB에서 읽어서 채움
String stock = redisTemplate.opsForValue().get("coupon:" + couponId + ":stock");
if (stock == null) {
    Coupon coupon = couponRepository.findById(couponId);
    redisTemplate.opsForValue().set("coupon:" + couponId + ":stock",
        String.valueOf(coupon.getTotalQuantity() - coupon.getIssuedQuantity()));
}
```

현재는 별도 `POST /api/coupons/{couponId}/stock/init` API로 총수량과 기간을 초기화한다. 자동 생성 훅이나 발급 요청의 cache-aside 재적재는 없다. 이미 발급된 쿠폰을 복구할 때는 총수량뿐 아니라 발급 완료 사용자와 처리 중 예약을 함께 고려해야 한다. 특히 Phase 3에서 DB의 `totalQuantity - issuedQuantity`만 쓰면 Kafka 대기 중인 예약을 놓친다.

### 8-3. Spring에서 Redis 사용하기 — RedisTemplate

```java
@RequiredArgsConstructor
@Service
public class CouponService {

    private final RedisTemplate<String, String> redisTemplate;
    // ...

    private String stockKey(Long couponId) {
        return "coupon:" + couponId + ":stock";
    }

    private String usersKey(Long couponId) {
        return "coupon:" + couponId + ":users";
    }
}
```

**재고 차감 — DECR:**
```java
Long remaining = redisTemplate.opsForValue().decrement(stockKey(couponId));

if (remaining == null || remaining < 0) {
    // 재고 초과 → 되돌리기
    redisTemplate.opsForValue().increment(stockKey(couponId));
    throw new CouponException(CouponErrorCode.EXHAUSTED);
}
```

**중복 체크 — SADD:**
```java
Long added = redisTemplate.opsForSet().add(usersKey(couponId), String.valueOf(userId));

if (added == null || added == 0) {
    throw new CouponException(CouponErrorCode.DUPLICATED);
}
```

> 현재는 `SADD`로 중복을 먼저 거른 뒤 `DECR`한다. 순서만 바꾼다고 장애 문제가 사라지는 것은 아니다. `SADD` 이후 실패하면 사용자 등록이 남을 수 있고 `SREM` 보상도 실패할 수 있으므로, 전체 예약 연산의 원자성과 실패 복구를 따로 설계해야 한다.

### 8-4. CouponService 변경 포인트

Phase 1의 `issueCoupon` 구조와 비교:

```java
// Phase 1
@Transactional
public void issueCoupon(Long couponId, Long userId) {
    Coupon coupon = couponRepository.findByIdWithLock(couponId);  // FOR UPDATE
    // 기간 체크, 재고 체크, 중복 체크 (모두 DB)
    coupon.setIssuedQuantity(coupon.getIssuedQuantity() + 1);
    issuedCouponRepository.save(...);
}

// Phase 2
// @Transactional은 IssuedCoupon INSERT에만 필요 (DB 쓰기 부분)
public void issueCoupon(Long couponId, Long userId) {
    // 1. 기간 체크 (DB or 캐시)
    validateCouponPeriod(couponId);

    // 2. 중복 체크 + 재고 차감 (Redis)
    checkDuplicateAndDecrStock(couponId, userId);

    // 3. MySQL에 발급 이력 저장 (성공한 경우만 도달)
    saveIssuedCoupon(couponId, userId);
}
```

`@Transactional` 범위가 줄어든다.  
Phase 1은 전체 로직이 하나의 트랜잭션 안에 있었지만,  
Phase 2는 Redis 처리는 트랜잭션 밖, MySQL 쓰기만 트랜잭션 안에 있다.

### 8-5. 재고 조회 API — Redis 조회 후 DB fallback

```java
// GET /api/coupons/{couponId}/stock
public Object getStock(Long couponId) {
    String cached = redisTemplate.opsForValue().get(stockKey(couponId));
    if (cached != null) {
        return Integer.parseInt(cached);  // Redis에서 바로 반환
    }
    // Redis Miss → DB 조회
    Coupon coupon = couponRepository.findById(couponId)
        .orElseThrow(() -> new CouponException(CouponErrorCode.NOT_FOUND));
    return coupon.getTotalQuantity() - coupon.getIssuedQuantity();
}
```

Phase 1에서는 재고 조회마다 DB를 쳤지만,  
Phase 2에서는 Redis에 값이 있으면 바로 반환하고, 없으면 DB를 조회한다. 이 fallback은 Redis를 재적재하지 않으며 예약 정보를 복구하는 기능도 아니다.

### 8-6. 테스트 시 주의사항

Phase 2 테스트에서 새로 고려해야 할 것들:

**1. Redis 초기화 필요 (k6 돌리기 전)**
```bash
# Redis CLI
docker exec -it redis redis-cli
FLUSHALL                           # 전체 초기화 (개발 환경만)
# 또는 특정 쿠폰만
DEL coupon:1:stock coupon:1:users
SET coupon:1:stock 100
```

**2. JUnit 테스트에서 Redis 상태 초기화**
```java
@BeforeEach
void setUp() {
    // DB 초기화
    issuedCouponRepository.deleteAll();
    couponId = couponRepository.save(coupon).getId();

    // Redis 초기화
    redisTemplate.delete("coupon:" + couponId + ":stock");
    redisTemplate.delete("coupon:" + couponId + ":users");
    redisTemplate.opsForValue().set("coupon:" + couponId + ":stock", "100");
}
```

**3. DB 정합성 확인**  
Redis에서 100건 성공했는데 MySQL에도 정확히 100건 INSERT됐는지 확인해야 한다.  
Redis만 보면 안 된다.

```java
assertThat(successCount.get()).isEqualTo(100);
assertThat(issuedCouponRepository.count()).isEqualTo(100);  // DB도 확인
// Redis 재고도 확인
String stock = redisTemplate.opsForValue().get("coupon:" + couponId + ":stock");
assertThat(Integer.parseInt(stock)).isEqualTo(0);
```

---

## 9. Phase 1 vs Phase 2 성능 비교

| 지표 | Phase 1 (DB Lock) | Phase 2 (Redis) |
|------|-------------------|--------------------|
| 평균 응답시간 | 1.2s | 947ms |
| p(95) | 2.32s | 1.39s |
| RPS | 409/s | 589/s |
| 발급 정확도 | 100/1000 ✅ | 100/1000 ✅ |

### 개선 과정 — 왜 세 번 고쳐야 했나

처음 Phase 2를 돌렸을 때 p(95)=2.09s로 Phase 1(2.32s)과 거의 차이가 없었다.  
Redis가 동작은 맞게 했는데 병목이 다른 곳에 있었다.

**1차 원인: `@Transactional` 범위가 너무 넓었다**

```java
@Transactional          // DB 커넥션 여기서 획득
public void issueCoupon(...) {
    findById(couponId);         // Step 1: DB 조회
    checkAndIssue(couponId);    // Step 2: Redis (커넥션 계속 점유 중)
    incrementIssuedQuantity();  // Step 3: DB 쓰기
}                       // DB 커넥션 여기서 반납
```

1000개 요청이 동시에 들어오면 Step 2(Redis)에서 900개가 실패하더라도,  
그 900개도 Step 1에서 이미 DB 커넥션을 잡아버린 상태다.  
HikariCP 기본 풀 사이즈 10 → 990개가 대기. Phase 1과 동일한 병목.

→ **`@Transactional` 제거 + `TransactionTemplate`으로 DB 쓰기만 감싸기**

**2차 원인: `findById`가 여전히 핫패스에 있었다**

`@Transactional`을 제거해서 커넥션 점유 시간은 줄었지만,  
`findById` 자체는 1000개 요청이 전부 호출한다.  
커넥션을 즉시 반납해도 풀 사이즈(10)보다 많은 요청이 동시에 DB를 치면 대기가 생긴다.

→ **쿠폰 기간 정보를 `initStock` 시점에 Redis에 올리고, Step 1을 Redis로 대체**

```
Before: 1000개 → findById(DB) → Redis → 900개 탈락
After:  1000개 → validatePeriod(Redis) → checkAndIssue(Redis) → 900개 탈락
                                                               → 100개만 DB 접근
```

### 당시의 병목 가설 — Tomcat 스레드 풀

초기 학습 기록의 p(95)=1.39s는 당시 500ms 목표에 못 미쳤다. 이 값은 [2026-09-12 비교](benchmark-2026-09-12.md) 및 10월 실험과 별도 기록이며, 여기에는 실행 조건과 원시 계측 자료가 연결되어 있지 않다.

당시에는 Tomcat 요청 처리 스레드 부족을 병목 후보로 보았다. 그러나 동시 요청 수만으로 스레드 대기 수를 확정할 수 없고, 스레드 사용량·큐 대기·DB 연결 대기·CPU를 함께 측정해야 원인을 구분할 수 있다.

`server.tomcat.threads.max=1000`으로 늘리는 것은 비교할 튜닝안이다. 지연이 줄어드는지는 같은 부하로 재측정해야 하며, DB·CPU 경합이 늘 수도 있다. Phase 3의 Kafka 전환은 HTTP 요청 경로에서 DB 저장 대기를 분리하는 대안이다. Redis 호출과 HTTP 요청 처리는 남으므로 스레드 풀 병목을 근본적으로 해결했다고 단정하지 않는다.

---

## 9. Phase 2의 한계 (Phase 3 예고)

Redis로 판정 경로를 옮겼지만 다음 문제는 별도로 남는다:

1. **Redis가 죽으면?** — 이미 AOF `everysec`와 RDB를 설정했지만 최근 쓰기 유실 가능성과 단일 노드 장애가 남는다. 복구 시 재고와 중복 정보, 처리 중 예약을 함께 대조해야 한다.
2. **DB 동기화** — Redis 예약 후 DB 실패 시 보상하며, 보상도 실패할 수 있다. Kafka를 넣으면 Redis 차감 후 발행 실패라는 새로운 간격이 생긴다.
3. **순서 보장** — 50번째 발급자의 기준이 API 도착인지, Redis 예약인지, Kafka 기록인지부터 정해야 한다. 파티션 안의 기록 순서가 곧 API 도착 순서는 아니다.
4. **확장 한계** — 쿠폰 수가 많을 때의 키 분산과 한 쿠폰의 hot key는 다르다. Kafka를 뒤에 붙여도 Redis에 도착하는 요청량은 줄지 않는다.

→ Phase 3는 Redis → Kafka → MySQL 비동기 저장을 구현한다. 장애 복구와 업무상 선착순 보장은 추가 설계·검증 과제다.

---

## 다음 글 예고

Phase 3에서는 Kafka를 Redis와 MySQL 사이에 두어 DB 쓰기를 분리하고, 파티션 순서와 실제 발급 순서의 관계를 다룬다.

> 다음: Phase 3. Kafka 비동기 처리 — 선착순 쿠폰 시스템 구축기 (4)
