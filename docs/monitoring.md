# 성능 테스트 & 모니터링
## k6 + InfluxDB + Prometheus + Grafana

> 기존 Phase 1~3에 같은 부하를 주고 HTTP 응답 처리량·지연과 DB·서버 지표를 함께 관측하는 안내다. 단계가 바뀌면 반드시 성능이 좋아진다고 가정하지 않는다.
> 성공·거절·접수를 합친 HTTP 요청 처리량과 최종 DB 발급 처리량은 다르다. [9월 측정 기록](benchmark-2026-09-12.md)은 HTTP 응답 기준이며, [10월 독립 실험](experiments-2026-10-22.md)은 접수 응답과 최종 발급 지연을 분리한다.

---

## 전체 구조

```
[k6 부하 테스트]
      ↓ 결과 메트릭
  [InfluxDB 1.8]          ← k6 전용 시계열 DB
      ↓
  [Grafana]               ← 대시보드 (포트 3000)
      ↑
  [Prometheus]            ← Spring Boot 메트릭 수집
      ↑ 5초마다 스크랩
  [Spring Boot Actuator]  ← /actuator/prometheus 엔드포인트 노출
```

---

## 도구별 역할

### k6 — 부하 테스트 도구

Grafana Labs에서 만든 오픈소스 부하 테스트 도구. JavaScript로 시나리오를 작성한다.

```javascript
export const options = {
  vus: 1000,        // Virtual Users (동시 사용자 수)
  duration: '30s',  // 테스트 지속 시간
};

export default function () {
  // 각 VU가 반복 실행하는 로직
  http.post('http://localhost:8080/api/coupons/1/issue', ...);
}
```

**주요 메트릭:**
| 메트릭 | 설명 |
|--------|------|
| `http_req_duration` | 요청 응답 시간 (p50, p95, p99) |
| `http_req_failed` | 실패 비율 (HTTP 오류 기준) |
| `http_reqs` | 초당 요청 수 (TPS, RPS) |
| `vus` | 현재 활성 Virtual User 수 |

**실행 방법:**
```bash
# k6 설치 (macOS)
brew install k6

# 테스트 실행 (결과를 InfluxDB로 전송)
k6 run --out influxdb=http://localhost:8086/k6 k6/phase1-concurrent-issue.js

# InfluxDB 없이 터미널 출력만
k6 run k6/phase1-concurrent-issue.js
```

---

### InfluxDB — k6 결과 저장소

k6가 테스트를 실행하면서 발생하는 메트릭을 시계열(time-series) 형태로 저장한다. 일반 RDB와 다르게 **시간 흐름에 따른 수치 데이터** 저장에 특화되어 있다.

이 프로젝트에서는 InfluxDB 1.8을 쓴다. k6의 `--out influxdb=` 옵션이 v1 포맷을 기본으로 지원하기 때문이다.

**접근:** http://localhost:8086

---

### Prometheus — Spring 앱 메트릭 수집기

Spring Boot Actuator가 노출하는 `/actuator/prometheus` 엔드포인트를 **5초마다 긁어서(scrape)** 저장한다.

```yaml
# prometheus/prometheus.yml
scrape_configs:
  - job_name: 'spring-boot'
    metrics_path: '/actuator/prometheus'
    static_configs:
      - targets: ['spring-app:8080']
    scrape_interval: 5s
```

**Spring에서 수집되는 주요 메트릭:**
| 메트릭 | 설명 |
|--------|------|
| `jvm_memory_used_bytes` | JVM 힙 메모리 사용량 |
| `jvm_gc_pause_seconds` | GC 발생 시간 |
| `hikaricp_connections_active` | DB 커넥션 풀 사용 중인 수 |
| `hikaricp_connections_pending` | 커넥션 대기 중인 스레드 수 |
| `http_server_requests_seconds` | API별 응답 시간 분포 |

`hikaricp_connections_pending`이 높다면 DB 커넥션 풀이 고갈되고 있다는 신호다. Phase 1(DB Lock)에서 이 수치가 튀는 것을 확인할 수 있다.

**접근:** http://localhost:9090

---

### Grafana — 통합 대시보드

InfluxDB와 Prometheus를 데이터 소스로 연결해서 시각화한다. `docker-compose up`으로 실행하면 두 데이터 소스가 자동 설정된다.

**접근:** http://localhost:3000 (admin / admin)

**k6 대시보드 추가 방법:**
1. Grafana 접속 → Dashboards → Import
2. Dashboard ID `2587` 입력 (공식 k6 대시보드)
3. Data Source: InfluxDB (k6) 선택

**Spring 대시보드 추가 방법:**
1. Dashboards → Import
2. Dashboard ID `4701` 입력 (JVM Micrometer 대시보드)
3. Data Source: Prometheus (Spring) 선택

---

## Phase별 테스트 시나리오

### Phase 1 — DB Lock

```bash
# 시나리오 1: 1000명 동시, 쿠폰 100개
k6 run --out influxdb=http://localhost:8086/k6 k6/phase1-concurrent-issue.js

# 시나리오 2: 동일 사용자 중복 방지
k6 run --out influxdb=http://localhost:8086/k6 k6/phase1-duplicate-check.js
```

**확인 포인트:**
- Grafana: `http_req_duration` p95 값 (응답 시간)
- Grafana: `hikaricp_connections_pending` (커넥션 대기) — DB Lock 때 높아짐
- 성공 수가 정확히 100인지

---

### Phase 2 — Redis

같은 k6 스크립트를 다시 실행해서 Phase 1과 비교한다.

**확인 포인트:**
- p95 응답 시간 감소 여부
- `hikaricp_connections_pending` 감소 여부 (Redis가 DB 부하를 흡수)
- TPS(초당 요청 처리량) 증가 여부

---

### Phase 3 — Kafka

```bash
# 202 접수 응답과 최종 DB 발급을 구분해 관측
k6 run --out influxdb=http://localhost:8086/k6 k6/phase1-concurrent-issue.js
```

**확인 포인트:**
- HTTP 응답 p95와 상태 코드별 분포 확인. 기존 Phase 3의 `202`는 Kafka ACK나 DB 발급 완료를 기다리지 않으므로 완료 지연으로 해석하지 않는다.
- Consumer lag 모니터링 (Kafka UI 또는 kafka-consumer-groups.sh)
- 요청 ID를 추적하는 후속 실험 모듈에서는 접수부터 최종 발급·거절까지의 지연도 별도로 기록한다. 기존 Phase 3의 k6 응답 지표만으로 이 값을 계산하지 않는다.

---

## Phase 비교 결과 기록 템플릿

각 Phase 테스트 후 아래 수치를 기록해두면 블로그 글에 쓰기 좋다:

| 지표 | Phase 1 (DB Lock) | Phase 2 (Redis) | Phase 3 (Kafka) |
|------|-----------------|-----------------|-----------------|
| HTTP 응답 p95 | - ms | - ms | - ms |
| HTTP 요청 처리량 (성공·거절·접수 포함), req/s | - | - | - |
| 최종 DB 발급 처리량 (별도 완료 계측 시), 건/s | - | - | - |
| 접수부터 최종 발급까지 p95 (별도 완료 계측 시) | - ms | - ms | - ms |
| 커넥션 대기 최댓값 | - | - | - |
| 성공 수 (100개 맞나) | - | - | - |
