# Redis 완전 정복
## 선착순 쿠폰 시스템으로 배우는 Redis

> 2026-10-22 보완: 현재 구현과 학습용 예제를 구분하고 원자성·영속성 설명을 정정했다. 다섯 가지 멘토링 키워드와 미결정 설계는 [멘토링 논의안](mentoring-discussion.md)에 정리한다.

> 실제 재현: Lua 예약·Stream relay는 실험 모듈(2026-10-26 main에서 제거, 코드는 [커밋 dc0e901](https://github.com/joejaeyoung/Project-coupon_rush/tree/dc0e901/4-coupon_experiments)), Lua 오류의 부분 쓰기·Redis 승격 시 쓰기 손실·Sentinel·Cluster 편중 부하는 [실증 보고서](experiments-2026-10-22.md)를 참고한다. 이 문서의 기존 Phase 2/3 설명과 구분한다. 현재 4단계 [4-coupon_redisHA_kafka](../4-coupon_redisHA_kafka/README.md)는 Lua·Stream 없이 Phase 3에 Kafka ACK 확인과 보상, DB 유니크 제약을 더한 최소 구성이다.

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

- **단일 명령의 동시 실행을 애플리케이션 락으로 막을 필요가 없다** — 여러 명령을 조합한 업무 흐름에는 여전히 경쟁 조건이 생길 수 있음
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
Thread A: DECR coupon:1:stock → 99 (100→99)
Thread B: DECR coupon:1:stock → 98 (99→98)  ← 순서대로 처리
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

### Lua Script — 명령어 조합의 원자성

개별 명령어의 원자성과 여러 명령어 조합의 원자성은 다르다.

`SADD`, `DECR` 각각은 원자적이다. 하지만 두 명령어 사이에는 **네트워크 왕복**이 있고, 그 사이에 다른 클라이언트가 끼어들 수 있다.

```
Java에서 두 명령어를 순서대로 호출할 때:

Spring A:  SADD users A  →  Redis 실행  →  1 반환
                              ↑ 다른 클라이언트 명령어가 여기 끼어들 수 있음
Spring A:  DECR stock    →  Redis 실행  →  결과 반환
```

**가장 흔한 문제 — "읽고 조건 판단 후 쓰기"**

```java
// Java 코드
String stock = redisTemplate.opsForValue().get(stockKey);  // GET → "1"
// ← 다른 클라이언트가 이 사이에 DECR → stock이 실제로는 이미 0
redisTemplate.opsForValue().decrement(stockKey);           // DECR → -1 (재고 없는데 차감!)
```

`GET`으로 읽은 값(1)과 실제 `DECR` 시점의 값(0)이 다르다. 읽기와 쓰기 사이에 다른 클라이언트가 바꿔버린 것이다.

**Lua Script로 해결:**

```lua
-- 재고가 있을 때만 차감하는 스크립트
local stock = redis.call('GET', KEYS[1])
if tonumber(stock) > 0 then
    return redis.call('DECR', KEYS[1])  -- GET과 DECR이 원자적으로 묶임
end
return -1  -- 재고 없으면 차감 자체를 안 함
```

Lua 스크립트는 **Redis 서버 안에서** 실행되며 실행 중 다른 클라이언트 명령이 끼어들지 못한다. 위 예제는 재고만 다루는 학습용 코드다. 쿠폰 예약에는 중복 확인·재고 확인·차감·사용자 등록을 함께 묶어야 한다. 이 원자성은 Kafka/MySQL과의 트랜잭션이나 장애 시 영속성을 보장하지 않는다. [Redis Lua 문서](https://redis.io/docs/latest/develop/programmability/eval-intro/)

**Lua가 필요한 경우 vs 아닌 경우:**

| 상황 | Lua 필요 여부 | 이유 |
|------|-------------|------|
| 단순 INCR/DECR | ❌ | 명령어 자체가 Read-Modify-Write를 원자적으로 처리 |
| SADD → 중복 체크 → DECR (현재 Phase 2/3) | 조합의 원자성이 필요하면 Lua 등으로 보완 | 개별 명령·보상 사이 장애로 불일치가 남을 수 있음 |
| GET → 조건 판단 → SET/DECR | ✅ | GET과 SET 사이에 값이 바뀔 수 있음 |
| 여러 키를 읽고 조건부로 여러 키를 수정 | ✅ | 중간 상태가 다른 클라이언트에 노출됨 |
| 복잡한 비즈니스 조건 (등급 체크 + 재고 + 중복) | ✅ | 3번 왕복 → 각 사이에 개입 가능 |

**Spring에서 Lua Script 실행:**

```java
// Lua 스크립트 정의
DefaultRedisScript<Long> script = new DefaultRedisScript<>();
script.setScriptText(
    "local stock = redis.call('GET', KEYS[1])\n" +
    "if tonumber(stock) > 0 then\n" +
    "  return redis.call('DECR', KEYS[1])\n" +
    "end\n" +
    "return -1"
);
script.setResultType(Long.class);

// 실행
Long result = redisTemplate.execute(script,
    List.of(stockKey),  // KEYS
    // ARGV 없음
);
```

**현재 Phase 2/3 구현과 남은 한계:**

```java
// SADD: 반환값 0이면 이미 추가된 것 → check-and-add가 원자적으로 해결됨
Long cnt = redisTemplate.opsForSet().add(usersKey, userId);
if (cnt == 0) throw DUPLICATED;

// DECR: 차감 후 음수이면 롤백 → DECR 자체가 원자적이라 -1이 정확하게 반환됨
Long left = redisTemplate.opsForValue().decrement(stockKey);
if (left < 0) { INCR; SREM; throw EXHAUSTED; }
```

기존 Phase 2/3 코드는 `SADD → DECR`를 별도로 호출하고, 음수면 `INCR → SREM`으로 보상한다. 정상 실행 중 단일 명령의 원자성을 활용하지만, 전체 예약 연산은 원자적이지 않다. `SADD` 직후 프로세스가 죽으면 사용자만 등록되고, 차감 뒤 다음 처리가 실패하면 발급되지 않은 예약이 남을 수 있다. 따라서 불일치가 항상 짧게 끝나는 것은 아니다. Phase 2의 DB 실패 보상도 Redis에서 별도 실행되므로 보상 자체의 실패를 고려해야 한다.

Redis Cluster에서 여러 키를 한 스크립트로 다루려면 같은 hash slot에 모아야 한다. 예를 들어 `coupon:{1}:stock`, `coupon:{1}:users`처럼 실제 중괄호가 들어간 hash tag를 사용할 수 있다. 기존 Phase 2/3 코드는 `coupon:1:stock` 형태이며 Cluster 설정도 없다. 후속 실험 모듈은 `lab:{runId:couponId}:` hash tag를 사용하고, 별도 Cluster 실험에서 슬롯 제약과 명령 분포를 확인했다. 이는 쿠폰 API 전체의 Cluster 연결·장애 전환 검증과는 다르다. 같은 쿠폰 키를 한 slot에 모으면 원자적 처리는 가능하지만 특정 쿠폰의 hot key 쓰기 부하는 한 노드에 남는다. [Redis Cluster 명세](https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/)

---

## 4. Redisson — Java에서 분산 락 구현

이 절은 대안 학습용이며 현재 Phase 2/3에는 Redisson 락이 구현되어 있지 않다.

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

**Watchdog 메커니즘**: lease time을 지정하지 않은 락에서 만료 시간을 갱신하는 기능이다. 위 예제는 lease time을 1초로 지정했으므로 작업이 더 길어지면 락이 풀릴 수 있다. [Redisson 락 문서](https://redisson.pro/docs/data-and-services/locks-and-synchronizers/)

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

위 TTL·cache-aside 흐름은 학습용 예제다. 현재 `initStock()`은 TTL 없이 재고와 기간을 저장한다. 잔여 수량 API는 Redis 키가 없으면 DB 값을 반환하지만 Redis를 다시 채우지 않는다. 특히 Phase 3의 재고는 처리 중 예약을 포함하므로 DB 값으로 무조건 덮어쓰면 안 된다.

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

현재 Redis는 재고와 중복 예약 여부를 판정하는 저장소다. Phase 2/3의 `redis/redis.conf`에는 이미 `appendonly yes`, `appendfsync everysec` 및 RDB 주기 설정이 있으며 Compose가 이 파일과 데이터 볼륨을 연결한다. 초당 fsync는 장애 시 최근 쓰기의 손실 가능성을 남긴다. [Redis 영속성 문서](https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/)

기존 Phase 2/3은 단일 Redis이며 replica·Sentinel·Cluster 구성이 없다. 후속 실험 모듈에서는 primary·replica의 수동 승격, Sentinel 자동 승격, replica 없는 3-master Cluster의 슬롯·편중 부하를 각각 실험했다. Spring의 Sentinel 자동 연결 전환과 Cluster 복제본 failover까지 검증한 것은 아니다. 비동기 복제에서는 승인된 쓰기가 failover 때 사라질 수 있으며 손실 반례도 [실증 보고서](experiments-2026-10-22.md)에 남겼다. [Redis Cluster 쓰기 안전성](https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/)

복구 시 DB의 발급 완료 기록만으로 재고를 재구성하면 Kafka 대기 중인 예약과 Redis 차감 후 Kafka에 기록되지 못한 예약을 놓친다. 재고·중복 정보·처리 중 요청을 함께 대조하는 복구 절차가 필요하다. 현재 `initStock()`은 총수량을 덮어쓰는 초기화 기능이며 이러한 장애 복구 기능이 아니다.

---

## 7. 이 프로젝트에서 Redis 활용 정리

| Phase | Redis 활용 | 명령어 |
|-------|-----------|-------|
| Phase 2/3 | 재고 차감·실패 시 보상 (각 명령은 원자적) | `DECR`, `INCR` |
| Phase 2/3 | 중복 예약 판정·실패 시 제거 | `SADD`, `SREM` |
| Phase 2/3 | 재고·기간 초기화 및 조회 (TTL 없음) | `SET`, `GET` |
| 후속 실험 모듈 | 요청 ID·중복·재고의 원자적 예약, Stream 전달 기록 | Lua, `XADD` — 기존 Phase 2/3와 분리된 구현 |
| 대안 학습 | 분산 락 | Redisson — 미구현 |

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
