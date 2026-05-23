# Phase 2. Redis로 동시성 제어
## 선착순 쿠폰 시스템 구축기 (3) — 오버셀링 아키텍처 진화

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

Scale-out(서버 증설), Scale-up(CPU/메모리 증설)을 해봤자 소용없다.  
**트래픽이 분산되지 않고 단 하나의 DB Row에 꽂히기 때문이다.**  
서버가 100대여도 결국 같은 Row에 Lock을 걸려고 줄을 선다.

샤딩(Sharding)도 해결이 안 된다. 재고는 쪼갤 수 없는 단일 자원이다.

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

Lua Script는 이 프로젝트의 Phase 2에서 선택적으로 쓸 수 있다.  
단순 재고 차감만이라면 `INCR` 명령어 하나로도 충분하다.  
조건이 복잡해질수록(쿠폰 적용, 사용자당 제한 수 등) Lua Script가 유리하다.

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

이 프로젝트의 Phase 2는 `INCR` + `SADD` 원자성으로 충분하다.  
Redisson은 비즈니스 로직이 복잡해지는 Phase 3 이후에 추가한다.

---

## 5. 재고 복구 문제 — 유령 재고

결제 창에 진입했다가 이탈한 사용자의 "선점된 재고"는 어떻게 복구하나.

**Redis TTL로 해결하면 안 되는 이유:**
```
SET coupon:1:lock:9999 "reserved" EX 300  ← 5분 TTL

문제:
1. TTL 만료는 정확히 5분이 아님
   → Redis는 Scan Event 기반으로 만료를 처리
   → CPU 여유가 있을 때 스캔함 → 수십 초 지연 가능
   → 1시간 이상 TTL은 그나마 안전, 수 분 단위 TTL은 타이밍 부정확

2. 발송 보장 없음 (ACK 없음)
   → 만료 이벤트를 구독하다가 처리 실패해도 재시도 불가
   → 재고가 복구 안 되고 유실됨

3. 서버 다운 시 복구 불가
   → Delayed MQ는 내부 스냅샷으로 백업, 서버 재시작해도 복구됨
   → Redis는 상대적으로 복구 신뢰성이 낮음
```

**Delayed MQ로 해결:**
```
재고 선점 시 → Delayed MQ에 "5분 후 복구" 메시지 발행
                              ↓ (5분 뒤)
              앱이 구독 중 → "결제 완료됐나 DB 확인"
              완료됨 → 아무것도 안 함
              미완료 → Redis INCR으로 재고 복구 + SREM으로 유저 제거
```

RabbitMQ는 순서 보장이 확실하고 ACK 기반이라 복구가 안전하다.  
Kafka도 설계에 따라 가능하지만 모든 상황에서의 순서 보장은 RabbitMQ가 우위다.

이 프로젝트는 선착순 쿠폰이라 "결제 대기" 개념이 없으므로  
Delayed MQ는 Phase 3(Kafka)에서 함께 다룬다.

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

Phase 2: Redis 원자적 연산
클라이언트 → Spring → Redis (INCR/SADD, 원자적)
                     → MySQL (Write-through)
            ↑ DB Lock 없음, 응답 수십ms, TPS 수만

Phase 3: Kafka 비동기 Write-Back
클라이언트 → Spring → Redis (즉시 응답)
                     → Kafka → MySQL (Throttling, DB 보호)
            ↑ 유저는 즉시 UX, DB는 안전하게 순차 처리

실서비스 Full:
클라이언트 → 대기열 (앞단 쓰로틀링)
           → Spring → Redisson 락 → Redis Lua Script
                    → Delayed MQ (재고 선점 타임아웃)
                    → 결제창 (PG)
                    → Kafka → MySQL
```

---

## 8. Phase 2 구현 상세 — 코드 레벨

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

  1. 기간 체크 (Redis 또는 DB, 캐시 가능)
  2. 중복 체크 → Redis SADD  ← 0이면 중복 → 즉시 반환
  3. 재고 차감 → Redis INCR  ← 총량 초과하면 DECR + SREM → 즉시 반환
  4. IssuedCoupon INSERT (MySQL)  ← DB 쓰기는 성공한 1건만
  5. 완료
  ↑
  DB Lock 없음. 2~3번은 μs 단위. DB 쓰기는 성공한 경우만.
```

### 8-2. Redis 초기화 — 쿠폰 재고를 언제 Redis에 올리나

쿠폰 발급 요청이 오기 전에 Redis에 재고를 세팅해야 한다.  
두 가지 방법이 있다:

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

이 프로젝트에서는 **방법 A**가 단순하다.  
다만 이미 발급된 쿠폰이 있다면 `totalQuantity - issuedQuantity`로 초기화해야 한다.

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

> 주의: SADD를 먼저 하고 DECR을 해야 한다.  
> DECR 먼저 하면 재고는 차감됐는데 SADD가 실패할 경우 재고만 줄고 발급은 안 된 상태가 된다.  
> 반대로 SADD 먼저 하면 DECR 실패 시 SREM으로 되돌릴 수 있다.

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

### 8-5. 재고 조회 API — Cache-aside

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
Phase 2에서는 Redis에서 바로 반환된다.

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

### 남은 병목 — Tomcat 스레드 풀

p(95)=1.39s까지 줄었지만 500ms 목표엔 못 미쳤다.  
지금 병목은 Tomcat 스레드 풀(기본 200개)이다.

1000개 요청이 동시에 들어오면 800개가 스레드를 못 받고 대기한다.  
Redis 로직이 아무리 빠르더라도 스레드를 못 받으면 의미가 없다.

`server.tomcat.threads.max=1000`으로 높이면 p(95)가 크게 줄지만,  
Phase 3(Kafka)에서 비동기 처리로 근본적으로 해결할 수 있으므로 여기서는 넘어간다.

---

## 9. Phase 2의 한계 (Phase 3 예고)

Redis로 응답 속도와 TPS는 해결됐다.  
하지만 남은 문제:

1. **Redis가 죽으면?** — 메모리 기반이라 장애 시 재고 데이터 유실 위험
2. **DB 동기화** — Redis에서 발급된 내역을 MySQL에 언제, 어떻게 쓸 것인가
3. **순서 보장** — 50번째 발급자에게 특별 이벤트를 주려면 정확한 순서가 필요

→ Phase 3에서 Kafka로 Redis → MySQL 비동기 Write-Back, 순서 보장, DB 보호를 구현한다.

---

## 다음 글 예고

Phase 3에서는 Kafka를 Redis와 MySQL 사이에 두어 폭발적인 Write 트래픽으로부터 DB를 보호하고, 이벤트 순서를 보장하는 방법을 다룬다.

> 다음: Phase 3. Kafka 비동기 처리 — 선착순 쿠폰 시스템 구축기 (4)
