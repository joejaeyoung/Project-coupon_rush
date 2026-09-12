# 부하 측정 기록 — 2026-09-12

세 Phase에 같은 시나리오를 걸어 비교한 원본 기록입니다. README의 표가 이 수치를 요약한 것입니다.

## 측정 조건

| 항목 | 값 |
| --- | --- |
| 머신 | Apple Silicon MacBook 1대 (앱 · DB · Redis · Kafka 전부 동일 호스트) |
| 부하 도구 | k6 v2.0.0, `shared-iterations`, 1,000 VU / 1,000 iterations, maxDuration 30s |
| 시나리오 | `POST /api/coupons/1/issue`, userId = `__VU` |
| 쿠폰 | `total_quantity` 100, 매 실행 전 `reset-coupon.sh 1 100`으로 재생성 |
| 앱 실행 | `./gradlew bootRun` (JDK 17), 기본 톰캣 · HikariCP 설정 |
| 인프라 | MySQL 8.0 · Redis 7 · Kafka 3.7.1(KRaft), 전부 단일 노드 |

앱과 인프라가 한 머신에 있으므로 **절대 처리량이 아니라 세 구현의 상대 비교**입니다.

## 결과

| 지표 | Phase 1 (DB 락) | Phase 2 (Redis) | Phase 3 (Redis + Kafka) |
| --- | ---: | ---: | ---: |
| `http_reqs` | 937.13 /s | 2,329.36 /s | 3,892.79 /s |
| `http_req_duration` avg | 753.10 ms | 122.65 ms | 102.18 ms |
| `http_req_duration` med | 787.93 ms | 109.55 ms | 96.72 ms |
| `http_req_duration` p90 | 946.42 ms | 142.23 ms | 130.15 ms |
| `http_req_duration` p95 | 967.75 ms | 303.08 ms | 158.57 ms |
| `http_req_duration` min | 83.22 ms | 66.27 ms | 64.37 ms |
| `http_req_duration` max | 991.96 ms | 415.21 ms | 227.50 ms |
| 총 소요 | 1.1 s | 0.4 s | 0.3 s |
| `coupon_issue_success` | 100 | 100 | 100 |
| `coupon_issue_fail` | 900 | 900 | 900 |

`http_req_failed` 90%는 **장애가 아니라 의도된 결과**입니다. 재고 100개에 1,000명이 요청했으므로 900건은 `409 Conflict`로 탈락하는 것이 정상입니다.

## 정합성 확인

Phase 2 · 3은 부하 직후 `report.sh 1`로 세 값을 대조했습니다. 모두 일치했습니다.

| 확인 대상 | Phase 2 | Phase 3 |
| --- | --- | --- |
| Redis 잔여 재고 | 0 | 0 |
| Redis 발급 유저 수 (`SCARD`) | 100 | 100 |
| MySQL `issued_coupon` 행 수 | 100 | 100 |
| MySQL `coupon.issued_quantity` | 100 | 100 |

Phase 3은 Consumer가 비동기로 INSERT하므로 부하 종료 직후가 아니라 몇 초 뒤에 대조해야 값이 맞습니다.

## 읽는 법

- **Phase 1 → 2에서 지연이 크게 떨어진 이유**는 탈락할 900건이 DB에 닿지 않기 때문입니다. Redis `SADD` · `DECR`에서 끝납니다.
- **Phase 2 → 3의 개선 폭이 상대적으로 작은 이유**는 이미 DB에 가는 요청이 100건뿐이라, 그 100건의 INSERT를 응답 경로에서 빼는 효과만 남기 때문입니다.
- **Phase 2의 p95(303ms)가 p90(142ms)보다 크게 튄 것**은 성공한 100건이 동기 INSERT를 기다린 구간으로 보입니다. Phase 3에서 이 꼬리가 158ms로 줄어든 것과 방향이 맞습니다. 다만 이 해석은 응답 분포를 성공/실패로 나눠 측정해 확인한 것이 아니라 **추정입니다.**

## 재현 방법

```bash
cd <phase>
docker compose up -d
cd spring && ./gradlew bootRun      # 다른 터미널
cd <phase> && ./reset-coupon.sh 1 100
k6 run -e COUPON_ID=1 -e USERS=1000 k6/concurrent-issue.js
./report.sh 1
```

Phase를 바꿀 때는 이전 Phase에서 `docker compose down`을 먼저 실행합니다. 세 Phase가 컨테이너 이름과 포트를 공유합니다.
