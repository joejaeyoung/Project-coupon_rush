# 레포 구조 재편 설계

**날짜:** 2026-05-24  
**목적:** Phase별 독립 실행 가능한 구조로 재편 후 GitHub 업로드

---

## 디렉토리 매핑

| 기존 | 신규 | 내용 |
|------|------|------|
| `coupon_db_lock/` | `1-coupon_db_lock/` | Phase 1 — DB Pessimistic Lock |
| `coupon_redis/` | `2-coupon_redis/` | Phase 2 — Redis 원자적 연산 |
| `coupon/` | `3-coupon_redis_kafka/` | Phase 3 — Redis + Kafka (현재 작업본 기반) |
| `docs/` | `docs/` | 공유 문서 (위치 변경 없음) |

---

## 각 Phase 디렉토리 구조

```
{phase}/
├── spring/                  ← Spring 앱 소스 (기존 최상위 내용 이동)
│   ├── src/
│   ├── build.gradle
│   ├── settings.gradle
│   ├── gradlew
│   └── Dockerfile
├── k6/
│   ├── concurrent-issue.js  ← 각 Phase 전용 k6 스크립트
│   └── duplicate-check.js   ← Phase 1만 해당
├── grafana/
│   └── provisioning/
├── prometheus/
│   └── prometheus.yml
├── mysql/
│   └── conf.d/
├── redis/                   ← Phase 2, 3만 해당
│   └── redis.conf
├── docker-compose.yml
├── reset-coupon.sh
└── run-k6.sh
```

---

## docker-compose 스택

| 서비스 | Phase 1 | Phase 2 | Phase 3 |
|--------|---------|---------|---------|
| MySQL | ✅ | ✅ | ✅ |
| Redis | ❌ | ✅ | ✅ |
| Kafka (KRaft) | ❌ | ❌ | ✅ |
| Spring App | ✅ (컨테이너) | ✅ (컨테이너) | ✅ (컨테이너) |
| Prometheus | ✅ | ✅ | ✅ |
| Grafana | ✅ | ✅ | ✅ |

**포트:** 모든 Phase 동일 (8080, 3306, 6379 등). 한 번에 하나만 실행.

---

## run-k6.sh 인터페이스

```bash
./run-k6.sh {couponId} {userCount}

# 예시
./run-k6.sh 1 1000
```

내부적으로 각 Phase의 `k6/concurrent-issue.js`를 호출.  
Phase 1은 `duplicate-check.js`도 별도 실행 가능하도록 선택 인자 추가 가능하지만 기본은 concurrent-issue.

---

## reset-coupon.sh 동작

| Phase | 동작 |
|-------|------|
| 1 | DB `issued_coupon` 삭제 + `coupon.issued_quantity=0` |
| 2 | DB 초기화 + Redis FLUSHDB + `POST /stock/init` 호출 |
| 3 | DB 초기화 + Redis FLUSHDB + `POST /stock/init` 호출 |

---

## Spring 컨테이너 빌드 방식

```yaml
# docker-compose.yml
spring-app:
  build:
    context: ./spring
    dockerfile: Dockerfile
  ports:
    - "8080:8080"
  depends_on:
    mysql:
      condition: service_healthy
```

Spring의 `application.yml`에서 DB/Redis 호스트는 서비스명으로 참조:
```yaml
datasource:
  url: jdbc:mysql://mysql:3306/coupon_db   # localhost → mysql
redis:
  host: redis                               # localhost → redis
```

---

## 제외 항목

- 기존 루트의 `coupon/`, `coupon_db_lock/`, `coupon_redis/` 디렉토리 삭제
- `load-test.html`, 루트의 `run-k6.sh`, `reset-coupon.sh` 삭제 (각 Phase 디렉토리로 이동)
- `spring/logs/`, `mysql/logs/` — .gitignore 처리
