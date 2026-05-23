# Redis 완전 정복
## 선착순 쿠폰 시스템으로 배우는 Redis

---

## 1. Redis가 뭔가요?

**Redis(Remote Dictionary Server)**는 데이터를 **메모리에 저장**하는 데이터 저장소다.

```
일반 DB (MySQL)   →  디스크에 저장  →  느림 (ms ~ 수십ms)
Redis             →  메모리에 저장  →  빠름 (μs 단위)
```

MySQL은 데이터를 디스크에 쓰고 읽는다. 디스크 I/O는 메모리 접근보다 수십~수백 배 느리다. Redis는 모든 데이터를 메모리에 올려두기 때문에 조회/수정이 마이크로초(μs) 단위로 빠르다.

### Redis가 빠른 또 다른 이유 — Single Thread

Redis는 **싱글 스레드**다. 명령어를 하나씩 순서대로 처리한다. 얼핏 보면 느릴 것 같지만 오히려 장점이 있다.

- **락(Lock)이 필요 없다** — 동시에 하나의 명령어만 실행되므로 경쟁 조건(Race Condition)이 없음
- **컨텍스트 스위칭 비용 없음** — 스레드 전환 오버헤드가 없음
- **원자성(Atomicity) 보장** — 모든 단일 명령어는 중간에 끊기지 않고 완전히 실행됨

이 **원자성**이 이 프로젝트에서 핵심이다. 뒤에서 자세히 설명한다.

---

## 2. Redis 자료구조

Redis는 단순한 Key-Value 저장소가 아니다. 5가지 핵심 자료구조를 지원한다.

### String — 가장 기본

```bash
SET key value         # 저장
GET key               # 조회
DEL key               # 삭제
INCR key              # 숫자 +1 (원자적)
DECR key              # 숫자 -1 (원자적)
INCRBY key 5          # 숫자 +5
EXPIRE key 60         # 60초 후 자동 삭제 (TTL)
TTL key               # 남은 만료 시간 조회
SETNX key value       # key가 없을 때만 SET (SET if Not eXists)
```

```bash
# 예시
SET coupon:1:stock 100
GET coupon:1:stock     # "100"
INCR coupon:1:stock    # 101 (원자적)
DECR coupon:1:stock    # 100 (원자적)
```

이 프로젝트에서: 쿠폰 잔여 수량 카운터로 사용

### Set — 중복 없는 집합

```bash
SADD key member       # 원소 추가 (중복 무시)
SREM key member       # 원소 삭제
SISMEMBER key member  # 원소 존재 여부 (1 or 0)
SMEMBERS key          # 모든 원소 조회
SCARD key             # 원소 개수
```

```bash
# 예시
SADD coupon:1:issued_users 1001  # 유저 1001이 발급받음
SADD coupon:1:issued_users 1002
SISMEMBER coupon:1:issued_users 1001  # 1 (이미 발급)
SISMEMBER coupon:1:issued_users 9999  # 0 (미발급)
```

이 프로젝트에서: 중복 발급 방지 (발급받은 userId 집합 관리)

### List — 순서 있는 목록

```bash
LPUSH key value   # 왼쪽에 추가
RPUSH key value   # 오른쪽에 추가
LPOP key          # 왼쪽에서 꺼냄
RPOP key          # 오른쪽에서 꺼냄
LRANGE key 0 -1   # 전체 조회
LLEN key          # 길이
```

Queue(FIFO): `RPUSH`로 넣고 `LPOP`으로 꺼냄
Stack(LIFO): `RPUSH`로 넣고 `RPOP`으로 꺼냄

### Hash — 객체 저장

```bash
HSET key field value    # 필드 설정
HGET key field          # 필드 조회
HGETALL key             # 모든 필드 조회
HDEL key field          # 필드 삭제
HINCRBY key field 1     # 필드 숫자 증가
```

```bash
# 예시 — 쿠폰 정보를 Hash로 저장
HSET coupon:1 name "신규가입 쿠폰" total_quantity 100 issued_quantity 0
HGET coupon:1 name            # "신규가입 쿠폰"
HINCRBY coupon:1 issued_quantity 1  # 원자적으로 발급 수 증가
```

### Sorted Set (ZSet) — 점수 기반 정렬 집합

```bash
ZADD key score member     # 원소 추가 (점수와 함께)
ZRANK key member          # 순위 조회 (낮은 점수가 0위)
ZRANGE key 0 -1           # 점수 오름차순 전체 조회
ZSCORE key member         # 특정 원소의 점수
```

```bash
# 예시 — 선착순 순위 기록
ZADD coupon:1:queue <timestamp> <userId>
ZRANK coupon:1:queue 1001  # 몇 번째로 요청했는지
```

---

## 3. 원자성(Atomicity)과 Race Condition

### Race Condition이란?

여러 클라이언트가 동시에 같은 데이터를 수정하면 예상치 못한 결과가 생긴다.

```
쿠폰 재고: 1개 남음

Thread A: GET stock → 1
Thread B: GET stock → 1  (A가 아직 안 줄였음)
Thread A: SET stock 0, 쿠폰 발급
Thread B: SET stock 0, 쿠폰 발급  ← 재고 없는데 발급됨! ❌
```

### INCR/DECR의 원자성

Redis의 `INCR`, `DECR`는 **Read-Modify-Write를 단 하나의 명령어로** 처리한다. 싱글 스레드이므로 두 클라이언트가 동시에 `DECR`를 해도 하나씩 순서대로 실행된다.

```
Thread A: DECR coupon:1:stock → 1 (100→99)
Thread B: DECR coupon:1:stock → 0 (99→98)  ← 순서대로 처리
```

```bash
# 선착순 발급 로직 (Redis 원자성 활용)
count = INCR coupon:1:issued_count
if count > total_quantity:
    DECR coupon:1:issued_count  # 되돌리기
    return "재고 소진"
else:
    return "발급 성공"
```

### SETNX와 분산 락

`SETNX`(SET if Not eXists)는 **키가 없을 때만 값을 설정**한다. 이것이 분산 락의 핵심이다.

```bash
# 락 획득 시도
SET lock:coupon:1 "locked" NX PX 3000
# NX: 키가 없을 때만 SET
# PX 3000: 3000ms(3초) 후 자동 만료

# 반환값
# "OK"   → 락 획득 성공
# (nil)  → 이미 다른 서버가 락을 갖고 있음
```

```
서버 A: SET lock:coupon:1 "A" NX PX 3000 → OK (락 획득)
서버 B: SET lock:coupon:1 "B" NX PX 3000 → nil (락 획득 실패, 대기)
서버 A: 발급 처리 완료
서버 A: DEL lock:coupon:1 (락 해제)
서버 B: SET lock:coupon:1 "B" NX PX 3000 → OK (락 획득)
```

`PX 3000` (만료 시간)이 없으면 서버 A가 락을 획득한 채로 죽었을 때 락이 영원히 풀리지 않는 **데드락**이 생긴다.

---

## 4. Redisson — Java에서 분산 락 구현

직접 `SETNX`를 구현하면 엣지 케이스가 많다. **Redisson** 라이브러리가 안전하게 처리해준다.

```java
RLock lock = redissonClient.getLock("coupon:lock:" + couponId);

try {
    // 최대 3초 대기, 락 유지 시간 1초
    boolean acquired = lock.tryLock(3, 1, TimeUnit.SECONDS);
    if (!acquired) {
        throw new CouponException(CouponErrorCode.EXHAUSTED);
    }
    // 임계 구역 — 동시에 하나의 스레드만 실행
    issueCoupon(couponId, userId);
} finally {
    if (lock.isHeldByCurrentThread()) {
        lock.unlock();
    }
}
```

**Watchdog 메커니즘**: Redisson은 락을 보유한 스레드가 살아있는 동안 자동으로 락 만료 시간을 갱신한다. 작업이 1초보다 오래 걸려도 락이 풀리지 않는다.

---

## 5. TTL(Time To Live) — 자동 만료

Redis의 모든 키에 만료 시간을 설정할 수 있다. **캐시**로 쓸 때 필수다.

```bash
SET coupon:1:stock 100
EXPIRE coupon:1:stock 60  # 60초 후 자동 삭제

# 또는 SET과 동시에
SET coupon:1:stock 100 EX 60
```

**Cache-aside 패턴**:
```
클라이언트 → Redis GET coupon:1:stock
              Cache Hit  → 바로 반환 ✅
              Cache Miss → MySQL 조회 → Redis SET (TTL 60s) → 반환
```

잔여 수량 조회 API에 이 패턴을 적용하면 DB 부하가 대폭 줄어든다.

---

## 6. Redis 영속성 — 메모리인데 데이터가 안 날아가나?

Redis가 메모리 저장이라 서버 재시작 시 데이터가 사라지는 것 아닌가? → 두 가지 영속성 옵션이 있다.

### RDB (Redis Database)
특정 시점의 스냅샷을 디스크에 저장. 주기적으로 백업.
- 장점: 파일 크기 작음, 재시작이 빠름
- 단점: 마지막 스냅샷 이후 데이터 유실 가능

### AOF (Append Only File)
모든 쓰기 명령어를 로그 파일에 순서대로 기록.
- 장점: 데이터 유실 최소화
- 단점: 파일 크기가 커짐, 재시작이 느림

이 프로젝트에서는 Redis를 캐시/락 용도로만 쓰기 때문에 영속성 설정이 크게 중요하지 않다. Redis가 재시작되면 캐시가 날아가고 DB에서 다시 채워지면 된다.

---

## 7. 이 프로젝트에서 Redis 활용 정리

| Phase | Redis 활용 | 명령어 |
|-------|-----------|-------|
| Phase 2 | 쿠폰 잔여 수량 원자적 제어 | `INCR`, `DECR` |
| Phase 2 | 중복 발급 방지 | `SADD`, `SISMEMBER` |
| Phase 2 | 분산 락 | `SET NX PX` (Redisson) |
| Phase 2 | 잔여 수량 캐시 | `GET`, `SET EX` |

---

## 8. 자주 쓰는 CLI 명령어 모음

```bash
# Redis 접속
docker exec -it redis redis-cli

# 키 관리
keys *                    # 모든 키 조회 (운영 환경에서는 사용 금지!)
keys coupon:*             # 패턴으로 조회
type key                  # 키의 자료구조 타입 확인
ttl key                   # 남은 만료 시간 (-1: 무기한, -2: 없는 키)
del key                   # 삭제
flushall                  # 전체 삭제 (테스트 초기화)

# 디버깅
monitor                   # 실시간으로 모든 명령어 출력 (개발용)
info memory               # 메모리 사용량 확인
info stats                # 통계 정보
```

---

## 9. Redis vs MySQL 언제 뭘 써야 하나?

| 상황 | Redis | MySQL |
|------|-------|-------|
| 조회 속도가 최우선 | ✅ | ❌ |
| 데이터가 영구 보존되어야 함 | ❌ | ✅ |
| 복잡한 쿼리 (JOIN 등) | ❌ | ✅ |
| 분산 락 | ✅ | △ (가능하나 느림) |
| 실시간 카운터 | ✅ | ❌ |
| 세션 저장 | ✅ | △ |
| 캐시 | ✅ | ❌ |

Redis는 MySQL을 **대체**하는 게 아니라 **보완**하는 것이다. "빠르게 읽어야 하는 것", "임시 데이터", "동시성 제어"는 Redis. "중요한 비즈니스 데이터의 영구 저장"은 MySQL.
