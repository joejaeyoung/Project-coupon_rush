# 부하 측정 기록 — 2026-09-12

세 Phase에 같은 k6 시나리오를 실행하고 측정값을 옮긴 기록입니다. 아래 수치는 당시 기록을 유지했습니다. 원시 k6 로그·JSON과 반복 측정 분산은 저장소에 포함되어 있지 않습니다.

## 측정 조건

| 항목 | 값 |
| --- | --- |
| 머신 | Apple Silicon MacBook 1대 (앱 · DB · Redis · Kafka 전부 동일 호스트) |
| 부하 도구 | k6 v2.0.0, `shared-iterations`, 1,000 VU / 1,000 iterations, maxDuration 30s |
| 시나리오 | `POST /api/coupons/1/issue`, userId = `__VU` |
| 쿠폰 | `total_quantity` 100, 매 실행 전 `reset-coupon.sh 1 100`으로 재생성 |
| 앱 실행 | `./gradlew bootRun` (JDK 17), 기본 톰캣 · HikariCP 설정 |
| 인프라 | MySQL 8.0 · Redis 7 · Kafka 3.7.1(KRaft), 전부 단일 노드 |

앱과 인프라가 한 머신에 있는 짧은 부하 기록입니다. 운영 처리량으로 일반화하지 않습니다. Phase 1·2는 DB 저장 후 `200`, Phase 3은 비동기 전송을 요청한 뒤 `202`를 반환하므로 **HTTP 응답 기준 비교**입니다. Phase 3 Producer는 전송 Future를 기다리지 않습니다.

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

지연과 `http_reqs`는 성공·접수·탈락을 합친 전체 요청 기준입니다. Phase 3의 수치를 접수부터 DB 저장까지의 지연 또는 최종 발급 처리량으로 읽으면 안 됩니다.

재고 소진·중복으로 반환하는 `409`도 `http_req_failed`에 포함됩니다. 다만 현재 `coupon_issue_fail`은 모든 비성공 응답을 세므로 **900건이라는 카운터만으로 전부 정상 `409`였다고 확인할 수는 없습니다.** 재현 시 상태·오류 코드별 분포를 함께 확인해야 합니다.

현재 스크립트의 `p95 < 300ms` 임계값은 기록상 Phase 1·2가 초과했습니다. 또한 `shared-iterations`는 VU별 실행 횟수를 균등하게 보장하지 않습니다. `userId = __VU`이므로 **1,000 iterations를 서로 다른 사용자 1,000명으로 단정하지 않습니다.** 서로 다른 사용자 1,000명의 수량 제어는 JUnit 테스트에서 별도로 다룹니다.

## 정합성 확인

Phase 2 · 3은 부하 직후 `report.sh 1`로 세 값을 대조했습니다. 모두 일치했습니다.

| 확인 대상 | Phase 2 | Phase 3 |
| --- | --- | --- |
| Redis 잔여 재고 | 0 | 0 |
| Redis 발급 유저 수 (`SCARD`) | 100 | 100 |
| MySQL `issued_coupon` 행 수 | 100 | 100 |
| MySQL `coupon.issued_quantity` | 100 | 100 |

Phase 3은 Consumer가 비동기로 INSERT하므로 부하 종료 직후가 아니라 몇 초 뒤에 대조해야 값이 맞습니다.

## 해석 범위

- 코드상 Phase 1은 모든 발급 요청이 쿠폰 행 락을 거칩니다. Phase 2는 중복·재고 소진 요청을 Redis에서 종료하고, 판정을 통과한 요청만 DB에 저장합니다.
- Phase 3은 그 DB 쓰기를 Consumer로 옮겨 접수 응답을 먼저 반환합니다. 응답 시간이 줄어든 기록은 이 경로 변경과 일치하지만, DB 저장 완료가 같은 비율로 빨라졌다는 증거는 아닙니다.
- Phase 2의 p95(303.08ms)가 p90(142.23ms)보다 큰 이유를 성공 요청의 동기 INSERT 대기라고 추정할 수 있습니다. 성공·탈락별 지연이나 DB 락 대기 시간을 따로 측정하지 않았으므로 **원인을 계측으로 분리한 결론은 아닙니다.**

## 재현 방법

Phase 3 예시입니다. 다른 단계는 [README의 서비스 목록](../README.md#직접-실행하기)을 사용합니다.

```bash
cd 3-coupon_redis_kafka
docker compose up -d mysql redis kafka
docker compose ps
```

인프라 기동 후 같은 터미널에서 앱을 실행합니다.

```bash
cd spring
./gradlew bootRun
```

새 터미널에서 앱 기동을 확인하고 초기화·측정·대조를 순서대로 실행합니다.

```bash
cd Project-coupon_rush/3-coupon_redis_kafka
curl --fail http://localhost:8080/actuator/health
./reset-coupon.sh 1 100
k6 run -e COUPON_ID=1 -e USERS=1000 k6/concurrent-issue.js
./report.sh 1
```

Phase 3은 Consumer 처리가 완료된 뒤 다시 대조합니다. 다음 초기화 전에 이전 실행의 이벤트 처리를 끝냅니다. 다른 Phase로 옮기기 전에는 로컬 앱을 종료하고 이전 Phase에서 `docker compose down`을 실행합니다.

## 참고

- [k6 shared-iterations 공식 설명](https://grafana.com/docs/k6/latest/using-k6/scenarios/executors/shared-iterations/): VU별 실행 횟수가 고정되지 않는 이유.
- [Spring Kafka 리스너 컨테이너](https://docs.spring.io/spring-kafka/reference/3.3/kafka/receiving-messages/message-listener-container.html): Kafka 자동 커밋과 컨테이너의 오프셋 커밋 관리 구분.
