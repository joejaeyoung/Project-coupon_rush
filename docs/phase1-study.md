# Phase 1. DB Lock으로 동시성 제어
## 선착순 쿠폰 시스템 구축기 (2)

> 1000명이 동시에 쿠폰 발급을 요청하면 어떻게 될까?
> 아무것도 하지 않으면 100개짜리 쿠폰이 150개 발급될 수도 있다.
> 이 글에서는 그 이유와, DB Lock으로 해결하는 방법을 다룬다.

---

## 1. 동시성 문제가 왜 생기나?

### Race Condition (경쟁 조건)

```
쿠폰 재고: 1개 남음

Thread A: SELECT issuedQuantity → 99
Thread B: SELECT issuedQuantity → 99  ← A가 아직 UPDATE 안 했음
Thread A: issuedQuantity < totalQuantity(100) → 발급 가능! → UPDATE issuedQuantity = 100
Thread B: issuedQuantity < totalQuantity(100) → 발급 가능! → UPDATE issuedQuantity = 100
```

둘 다 "99 < 100"을 확인하고 발급을 진행한다. 결과적으로 재고가 0인데 2명이 발급받는다.

이것이 **Race Condition** — 두 스레드가 동시에 같은 자원을 읽고 수정할 때 발생하는 문제다.

---

## 2. 트랜잭션과 격리 수준

### 트랜잭션이란?

**트랜잭션**은 "모두 성공하거나, 모두 실패하거나"를 보장하는 작업 단위다.

```sql
BEGIN;
  UPDATE coupon SET issued_quantity = issued_quantity + 1 WHERE id = 1;
  INSERT INTO issued_coupon (coupon_id, user_id, status) VALUES (1, 1001, 'ISSUED');
COMMIT;  -- 둘 다 성공해야 반영
-- 중간에 오류 나면 ROLLBACK → 둘 다 취소
```

트랜잭션의 4가지 특성 **ACID**:
- **A**tomicity (원자성): 전부 실행 or 전부 취소
- **C**onsistency (일관성): 트랜잭션 전후로 데이터 규칙이 유지됨
- **I**solation (격리성): 동시에 실행 중인 트랜잭션끼리 서로 영향 안 줌
- **D**urability (지속성): 커밋된 데이터는 장애가 나도 유지됨

### 격리 수준 (Isolation Level)

트랜잭션의 **격리성**을 얼마나 엄격하게 적용할지 설정하는 레벨이다. 엄격할수록 안전하지만 느려진다.

| 격리 수준 | Dirty Read | Non-Repeatable Read | Phantom Read | 성능 |
|---------|-----------|---------------------|--------------|------|
| READ UNCOMMITTED | 발생 | 발생 | 발생 | 가장 빠름 |
| READ COMMITTED | 방지 | 발생 | 발생 | |
| **REPEATABLE READ** (MySQL 기본) | 방지 | 방지 | 방지* | |
| SERIALIZABLE | 방지 | 방지 | 방지 | 가장 느림 |

각 이상 현상의 의미:

**Dirty Read**: 커밋되지 않은 데이터를 다른 트랜잭션이 읽는 것
```
A: UPDATE coupon SET stock = 0  (아직 커밋 안 함)
B: SELECT stock → 0  ← 커밋 전 데이터를 읽어버림
A: ROLLBACK  → stock은 다시 100
B: 0이라고 판단해서 잘못된 처리
```

**Non-Repeatable Read**: 같은 트랜잭션에서 같은 행을 두 번 읽었는데 값이 다른 것
```
A: SELECT stock → 100
B: UPDATE stock = 0 → COMMIT
A: SELECT stock → 0  ← 같은 트랜잭션인데 값이 달라짐
```

**Phantom Read**: 같은 조건으로 두 번 조회했는데 행 개수가 달라지는 것
```
A: SELECT COUNT(*) WHERE status='ACTIVE' → 5
B: INSERT INTO coupon (status='ACTIVE') → COMMIT
A: SELECT COUNT(*) WHERE status='ACTIVE' → 6  ← 유령 행이 생김
```

MySQL의 기본값인 **REPEATABLE READ**는 Non-Repeatable Read까지 방지한다. 같은 트랜잭션에서 같은 쿼리를 여러 번 날려도 처음 읽은 스냅샷을 계속 반환한다.

---

## 3. Lock — 동시성 제어의 핵심

### 왜 Lock이 필요한가?

트랜잭션 격리 수준만으로는 Race Condition을 완전히 막지 못한다. REPEATABLE READ도 **읽기에 대한 일관성**을 보장할 뿐, 두 트랜잭션이 동시에 수정할 때는 여전히 문제가 생긴다.

Lock은 **특정 자원에 대한 접근을 순서대로** 만들어서 Race Condition을 막는다.

### Pessimistic Lock (비관적 락)

"어차피 충돌이 날 것이다" — **미리 잠금**을 걸어두는 방식.

```sql
SELECT * FROM coupon WHERE id = 1 FOR UPDATE;
-- 이 Row는 내 트랜잭션이 끝날 때까지 다른 트랜잭션이 수정 불가
```

`FOR UPDATE`가 붙으면 MySQL이 해당 Row에 **배타적 Lock(Exclusive Lock)**을 건다.

```
Thread A: SELECT ... FOR UPDATE  → Lock 획득
Thread B: SELECT ... FOR UPDATE  → Lock 대기 (A가 COMMIT/ROLLBACK 할 때까지 블로킹)
Thread A: UPDATE issuedQuantity = 100 → COMMIT → Lock 해제
Thread B: Lock 획득 → SELECT → issuedQuantity = 100 확인 → 재고 없음 → 예외 발생
```

이렇게 하면 한 번에 하나씩 순서대로 처리된다. 재고 초과 발급이 불가능해진다.

**JPA에서 Pessimistic Lock 사용:**

```java
@Lock(LockModeType.PESSIMISTIC_WRITE)
@Query("SELECT c FROM Coupon c WHERE c.id = :id")
Optional<Coupon> findByIdWithLock(@Param("id") Long id);
```

`PESSIMISTIC_WRITE` = `SELECT ... FOR UPDATE`와 동일하다.

**단점:**
- Lock을 기다리는 동안 DB 커넥션을 점유 → 커넥션 풀 고갈 위험
- 대기 시간이 길어져 응답 속도 저하
- 잘못 설계하면 데드락 발생

### Optimistic Lock (낙관적 락)

"충돌이 드물 것이다" — 잠금 없이 처리하다가 **충돌 시 감지**하는 방식.

```java
@Entity
public class Coupon {
    @Version
    private Integer version;  // 버전 컬럼 추가
}
```

```sql
-- 조회 시: version = 5
-- 업데이트 시:
UPDATE coupon SET issued_quantity = 100, version = 6
WHERE id = 1 AND version = 5  -- 버전이 맞아야만 업데이트
```

Thread A와 B가 동시에 version=5를 읽었다면:
- A가 먼저 UPDATE → version이 6이 됨
- B가 UPDATE 시도 → `WHERE version = 5`가 매칭 안 됨 → 0 rows affected
- JPA가 `OptimisticLockException` 발생

**이 과제에는 부적합한 이유:**
선착순 쿠폰은 수천 명이 동시에 요청한다. 충돌이 매우 잦기 때문에 `OptimisticLockException`이 계속 발생하고, 재시도 로직이 복잡해진다. 충돌이 드문 상황(상품 정보 수정 등)에 적합하다.

### 공유 락 vs 배타 락

락의 두 가지 종류를 알아두면 좋다:

| 락 종류 | SQL | 다른 트랜잭션이 읽기 | 쓰기 |
|---------|-----|-------------------|------|
| 공유 락 (Shared Lock) | `SELECT ... LOCK IN SHARE MODE` | 가능 | 불가 |
| 배타 락 (Exclusive Lock) | `SELECT ... FOR UPDATE` | 불가 | 불가 |

`PESSIMISTIC_WRITE`는 배타 락이다. 내가 잠그면 다른 트랜잭션은 읽지도 못한다.

---

## 4. 데드락 (Deadlock)

락을 쓸 때 반드시 알아야 하는 문제다.

```
Thread A: coupon(id=1) 잠금 → order(id=1) 잠금 시도 중
Thread B: order(id=1) 잠금 → coupon(id=1) 잠금 시도 중

A는 B가 order를 놓기를 기다리고
B는 A가 coupon을 놓기를 기다림
→ 둘 다 영원히 대기 → 데드락
```

**MySQL의 데드락 처리:**
MySQL은 데드락을 감지하면 한쪽 트랜잭션을 자동으로 롤백하고 에러를 반환한다.
```
ERROR 1213 (40001): Deadlock found when trying to get lock; try restarting transaction
```

**데드락 예방:**
- **Lock 획득 순서를 항상 일정하게** — coupon → issued_coupon 순서로 잠그면, 모든 트랜잭션이 같은 순서로 시도하므로 데드락이 안 생김
- **트랜잭션을 짧게** — 락을 오래 보유할수록 데드락 가능성이 높아짐
- **필요한 Row만 잠금** — `WHERE id = ?`로 특정 Row만 잠가야지, 전체 테이블을 잠그면 안 됨

---

## 5. @Transactional 주의사항

Pessimistic Lock은 **트랜잭션 안**에서만 동작한다. Lock은 트랜잭션이 커밋/롤백될 때 해제되기 때문이다.

```java
// ❌ 트랜잭션 없으면 Lock이 즉시 해제됨 — 의미 없음
public void issueCoupon(Long couponId) {
    Coupon coupon = couponRepository.findByIdWithLock(couponId); // Lock 걸렸다가
    // 메서드 끝나면 Lock 해제 → 다음 줄에서 수정할 때는 이미 Lock이 없음
    coupon.setIssuedQuantity(...);
}

// ✅ @Transactional이 있어야 Lock이 트랜잭션 끝까지 유지됨
@Transactional
public void issueCoupon(Long couponId) {
    Coupon coupon = couponRepository.findByIdWithLock(couponId); // Lock 획득
    coupon.setIssuedQuantity(...); // 처리
    // 트랜잭션 커밋 → Lock 해제
}
```

---

## 6. 동시성 테스트 — CountDownLatch

단위 테스트에서 동시성을 재현하려면 여러 스레드를 "동시에" 출발시켜야 한다. 그냥 for문으로 반복하면 순차 실행이다.

### CountDownLatch란?

카운트가 0이 될 때까지 스레드를 **대기**시키는 동기화 도구다.

```java
CountDownLatch latch = new CountDownLatch(1);

// 스레드 10개가 모두 대기 상태
for (int i = 0; i < 10; i++) {
    new Thread(() -> {
        latch.await();  // 카운트가 0이 될 때까지 대기
        // 실제 작업
    }).start();
}

latch.countDown();  // 카운트 1 → 0 → 대기 중인 10개 스레드 동시 출발
```

### ExecutorService

스레드 풀을 관리하는 도구. 매번 `new Thread()`를 만들면 비용이 크기 때문에 풀에서 재사용한다.

```java
ExecutorService executor = Executors.newFixedThreadPool(32);
// 최대 32개 스레드를 재사용. 1000개 작업을 32개씩 나눠서 처리
executor.submit(() -> { /* 작업 */ });
executor.shutdown(); // 더 이상 작업 제출 안 함, 진행 중인 작업은 완료 대기
```

### AtomicInteger — 스레드 안전한 카운터

여러 스레드가 동시에 `count++`를 하면 Race Condition이 생긴다. `AtomicInteger`는 증가 연산이 원자적으로 보장된다.

```java
// ❌ 일반 int — 동시 접근 시 Race Condition
int count = 0;
count++;  // read → modify → write 가 원자적이지 않음

// ✅ AtomicInteger — 원자적으로 증가
AtomicInteger count = new AtomicInteger(0);
count.incrementAndGet();  // 원자적
```

### 테스트 전체 구조

```java
int totalUsers  = 1000;
ExecutorService executor    = Executors.newFixedThreadPool(32);
CountDownLatch  startLatch  = new CountDownLatch(1);    // 동시 출발 신호
CountDownLatch  doneLatch   = new CountDownLatch(totalUsers); // 완료 대기
AtomicInteger   successCount = new AtomicInteger(0);

for (int i = 0; i < totalUsers; i++) {
    long userId = i + 1;
    executor.submit(() -> {
        try {
            startLatch.await();  // 신호 올 때까지 대기
            couponService.issueCoupon(couponId, userId);
            successCount.incrementAndGet();
        } catch (CouponException e) {
            // 재고 소진, 중복 등 예상된 예외는 무시
        } finally {
            doneLatch.countDown();  // 완료 신호
        }
    });
}

startLatch.countDown();  // 1000개 스레드 동시 출발!
doneLatch.await();       // 전부 끝날 때까지 메인 스레드 대기

assertThat(successCount.get()).isEqualTo(100);  // 정확히 100개만 성공해야 함
```

---

## 7. 발급 로직 흐름 정리

```
POST /api/coupons/{couponId}/issue?userId=1001

  1. findByIdWithLock(couponId)
     → SELECT * FROM coupon WHERE id = ? FOR UPDATE
     → 이 Row에 배타 락 획득. 다른 요청은 여기서 대기.

  2. 발급 기간 체크
     → startDate <= now <= endDate ?
     → No  → CouponNotAvailableException (400)

  3. 재고 체크
     → issuedQuantity < totalQuantity ?
     → No  → CouponExhaustedException (409)

  4. 중복 발급 체크
     → existsByCoupon_IdAndUserId(couponId, userId) ?
     → Yes → DuplicateCouponIssueException (409)

  5. issuedQuantity + 1
     → Dirty Checking으로 자동 UPDATE

  6. IssuedCoupon 저장
     → status: ISSUED
     → expiredAt: now + validDays

  7. 트랜잭션 커밋 → Lock 해제 → 다음 요청이 Lock 획득
```

---

## 8. Phase 1의 한계 (Phase 2 예고)

Pessimistic Lock으로 정확성은 보장되지만 문제가 있다.

**커넥션 풀 고갈:**
```
DB 커넥션 풀: 10개

스레드 1000개가 동시에 요청
→ 10개는 Lock 획득 후 처리 중
→ 990개는 Lock 대기 + 커넥션 점유
→ 커넥션 풀이 990개를 감당 못 함 → 에러
```

HikariCP 기본 커넥션 풀은 10개다. 1000개 동시 요청을 처리하기엔 부족하다.

**응답 속도:**
Lock을 기다리는 시간이 길어질수록 p95, p99 응답 시간이 늘어난다.

→ Phase 2에서 Redis로 이 문제를 해결한다.
```
Redis INCR (원자적 연산)으로 수량 제어
→ DB Lock이 필요 없음 → 커넥션 풀 고갈 없음
→ Redis가 메모리 기반이라 응답이 훨씬 빠름
```

---

## 9. k6 결과 해석 — checks 66.66%가 실패인가?

k6를 처음 돌리면 아래 결과가 헷갈린다.

```
checks_succeeded...: 66.66% 2000 out of 3000
checks_failed......: 33.33% 1000 out of 3000

✗ status 200 (발급 성공) ↳ 10%  — ✓ 100 / ✗ 900
✗ status 409 (소진/중복) ↳ 90%  — ✓ 900 / ✗ 100
✓ status is not 500
```

"실패 33%"처럼 보이지만 **정상 결과**다.

**이유:** check는 "하나의 요청이 모든 조건을 동시에 만족해야 성공"으로 계산된다.

- 200 받은 요청 → `status 200` ✓, `status 409` **✗** → 1개 실패로 계산
- 409 받은 요청 → `status 200` **✗**, `status 409` ✓ → 1개 실패로 계산

모든 요청이 둘 중 하나는 반드시 실패한다. 수학적으로:
```
총 3000번 체크 = 1000요청 × 3개 조건
성공: (100 + 900) + 1000 = 2000 → 66.67%
```

**핵심:** 비즈니스 정합성은 `coupon_issue_success = 100`으로 확인한다. checks 비율은 부수적 지표일 뿐이다.

---

## 10. CORS — 브라우저와 k6의 차이

같은 API를 호출하는데 k6는 성공하고 브라우저(HTML)는 `Failed to fetch`가 뜨는 이유가 있다.

**CORS(Cross-Origin Resource Sharing)** 는 브라우저가 사용자 보호를 위해 강제하는 정책이다.

```
브라우저에서 file://index.html → localhost:8080 요청
→ 브라우저: "출처가 다르다. 서버가 허용했는지 확인"
→ 서버가 Access-Control-Allow-Origin 헤더 없음
→ 브라우저가 응답을 차단 → Failed to fetch
```

k6, curl, Postman은 **브라우저가 아니기 때문에** CORS 정책 자체가 없다. HTTP 클라이언트로 직접 요청한다.

**Spring에서 CORS 허용:**

```java
@CrossOrigin(origins = "*")  // 모든 출처 허용 (개발 환경)
@RestController
public class CouponController { ... }
```

`origins = "*"`는 개발 환경에서만 쓴다. 실서비스에서는 허용할 도메인을 명시해야 한다.

---

## 11. 엔티티 필드 초기값 — 테스트에서 자주 만나는 NPE

JPA 엔티티에서 `Integer`, `Long` 같은 래퍼 타입은 명시적으로 초기화하지 않으면 `null`이다.

```java
@Entity
public class Coupon {
    private Integer issuedQuantity;  // 초기화 안 하면 null
    private Integer validDays;       // 초기화 안 하면 null
}
```

서비스 코드에서 null인 채로 연산하면 NPE가 발생한다.

```java
// issuedQuantity가 null이면 자동 언박싱 → NullPointerException
if (coupon.getIssuedQuantity() >= coupon.getTotalQuantity()) { ... }

// validDays가 null이면 → NullPointerException
issuedCoupon.setExpiredAt(now.plusDays(coupon.getValidDays()));
```

**해결 방법 두 가지:**

```java
// 방법 1: 엔티티에 기본값 지정
private Integer issuedQuantity = 0;
private Integer validDays = 30;

// 방법 2: 테스트 setUp에서 반드시 명시
coupon.setIssuedQuantity(0);
coupon.setValidDays(30);
```

실서비스에서는 방법 1이 안전하다. DB에 `DEFAULT 0`을 설정해두고, 엔티티 필드에도 기본값을 주면 어디서 생성해도 null이 안 생긴다.

---

## 다음 글 예고

Phase 2에서는 Redis의 원자적 연산으로 DB Lock 없이도 동시성을 제어하고, Grafana로 Phase 1 vs Phase 2 응답 시간을 직접 비교한다.

> 다음: Phase 2. Redis 분산 락 — 선착순 쿠폰 시스템 구축기 (3)

![[Pasted image 20260523152336.png]]

```sh
  █ THRESHOLDS

    http_req_duration
    ✗ 'p(95)<2000' p(95)=2.32s

    http_req_failed
    ✓ 'rate<0.95' rate=90.00%


  █ TOTAL RESULTS

    checks_total.......: 3000   1228.409676/s
    checks_succeeded...: 66.66% 2000 out of 3000
    checks_failed......: 33.33% 1000 out of 3000

    ✗ status 200 (발급 성공)
      ↳  10% — ✓ 100 / ✗ 900
    ✗ status 409 (소진/중복)
      ↳  90% — ✓ 900 / ✗ 100
    ✓ status is not 500

    CUSTOM
    coupon_issue_fail..............: 900    368.522903/s
    coupon_issue_success...........: 100    40.946989/s

    HTTP
    http_req_duration..............: avg=1.2s     min=303.38ms med=1.11s    max=2.38s    p(90)=2.29s    p(95)=2.32s
      { expected_response:true }...: avg=522.36ms min=303.38ms med=509.12ms max=836.45ms p(90)=678.21ms p(95)=693.57ms
    http_req_failed................: 90.00% 900 out of 1000
    http_reqs......................: 1000   409.469892/s

    EXECUTION
    iteration_duration.............: avg=1.25s    min=313.81ms med=1.17s    max=2.43s    p(90)=2.32s    p(95)=2.38s
    iterations.....................: 1000   409.469892/s
    vus............................: 162    min=162         max=767
    vus_max........................: 1000   min=1000        max=1000

    NETWORK
    data_received..................: 179 kB 73 kB/s
    data_sent......................: 152 kB 62 kB/s




running (0m02.4s), 0000/1000 VUs, 1000 complete and 0 interrupted iterations
concurrent_users ✓ [==========================] 1000 VUs  02.4s/30s  1000/1000 shared iters
ERRO[0002] thresholds on metrics 'http_req_duration' have been crossed
```

![[Pasted image 20260523155813.png]]

![[Pasted image 20260523160028.png]]