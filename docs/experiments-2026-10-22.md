# 선착순 쿠폰 설계 실증 — 2026-10-22

멘토링에서 정리한 다섯 질문을 독립된 [실험 모듈](../4-coupon_experiments/README.md)로 구현하고 실제 MySQL·Redis·Kafka를 사용해 검증했다. 정상 발급 수뿐 아니라 예약 후 미발행, 재처리, 처리 순서, 복제 지연 중 승인된 쓰기 손실을 관찰했다.

**기록 기준:** 아래 표는 실제 실행을 마치고 원본 결과를 확인한 실험이다. 실패 반례를 재현한 실험과 문제를 방지한 실험을 구분하며, 측정 범위 밖의 운영 보장으로 확대하지 않는다.

## 1. 무엇을 구현했고 무엇을 비교했나

기존 Phase 1~3 소스와 2026-09-12 측정 기록은 보존하고 `4-coupon_experiments/`를 추가했다. 새 실험에는 다음 다섯 경로가 있다.

| 전략 | 요청 처리 경로 | 비교하려는 문제 |
| --- | --- | --- |
| `SPLIT_REDIS` | `SADD`·`DECR` 개별 호출 → Kafka → DB | 분리 명령 사이 실패, 예약과 발행 사이 공백 |
| `LUA_REDIS` | Lua로 중복·재고·요청 ID 판정 → Kafka → DB | Redis 내부 판정을 묶어도 남는 외부 발행 공백 |
| `KAFKA_FIRST` | Kafka ACK → Consumer의 Lua 판정 → DB | 로그 순서에 따른 판정, 재처리, 접수와 최종 결과의 차이 |
| `DB_OUTBOX` | DB 예약·재고 차감·Outbox 동시 커밋 → relay → Kafka → DB | 예약과 발행할 기록을 함께 보존하는 복구 |
| `REDIS_STREAM` | 같은 슬롯의 Lua 예약·`XADD` → relay → Kafka → DB | 예약과 전달 기록을 Redis에서 함께 생성하는 복구 |

공통으로 **API가 동기식 MySQL 요청 원장을 먼저 기록**한다. Kafka 메시지는 이 원장의 요청 ID를 참조한다. 다섯 경로 모두 DB 총량 제한과 `(runId, couponId, userId)` 유일 제약을 적용하고, 발급 행·결과 상태를 같은 최종 트랜잭션에서 확정한다. `DB_OUTBOX`는 앞선 예약 트랜잭션에서 재고·예약·이벤트를 함께 커밋하고 최종 발급 시 다시 차감하지 않는다. 나머지는 최종 발급 트랜잭션에서 DB 가용량도 차감한다. 이 공통 방어 장치가 있는 실험에서 관찰한 초과 발급 0을 기존 Phase 3이나 Redis 단독 설계의 보장으로 확대할 수 없다. 구현 경계는 [LabService](../4-coupon_experiments/spring/src/main/java/com/example/couponlab/LabService.java), [LabStore](../4-coupon_experiments/spring/src/main/java/com/example/couponlab/LabStore.java)에 있다.

요청 원장은 관측과 재시도 식별에 쓰지만, `SPLIT_REDIS`·`LUA_REDIS`의 미발행 예약을 자동 스캔하여 발행하지 않는다. 이 두 경로는 같은 요청 ID로 명시적으로 재시도해야 이어서 진행한다. 따라서 요청 원장이 있다는 이유만으로 Outbox와 같은 자동 복구 경로로 분류하지 않았다.

### 완료된 증거

| 실험 묶음 | 결과 | 증거 |
| --- | --- | --- |
| 기존 Phase 1~3 동시성 테스트 | 최종 재실행 6/6 통과 | [원본 구현 재검증](../4-coupon_experiments/results/original-baselines.json) |
| 정상·중복·요청 ID·식별자 경계 | 20/20 케이스, 검증 항목 360개 통과 | [correctness-v2](../4-coupon_experiments/results/correctness-v2/summary.json) |
| 예외 주입·재처리·일시 정지·취소 | 27/27 케이스, 검증 항목 476개 통과 | [faults-v2](../4-coupon_experiments/results/faults-v2/summary.json) |
| 도착·기록·완료 순서와 Consumer 재할당 | 4/4 케이스, 검증 항목 111개 통과 | [ordering](../4-coupon_experiments/results/ordering/summary.json) |
| 트래픽 분포·파티션·Consumer 조합 | 135/135, 측정 40,500건과 warmup 2,700건 완료 | [distribution](../4-coupon_experiments/results/distribution/summary.json) |
| API JVM 1개·2개 비교 | 8/8, 측정 8,000건과 warmup 160건 완료 | [api-scaling](../4-coupon_experiments/results/api-scaling.json) |
| JVM SIGKILL·재시작 / 발급·취소 경쟁 | 다섯 경로 복구 관측 통과 / 60건 중 발급 30·취소 30 | [process-recovery](../4-coupon_experiments/results/process-recovery.json) |
| Kafka 리더 SIGKILL | ACK된 대기 1건 + 장애 중 40건, 최종 고유 발급 41건 | [kafka-leader-failover](../4-coupon_experiments/results/kafka-leader-failover.json) |
| Kafka 2노드 중단 중 다섯 경로 | 각 경로의 접수·예약·자동 복구/동일 ID 재시도 5/5 통과 | [kafka-outage-paths](../4-coupon_experiments/results/kafka-outage-paths.json) |
| MySQL 서비스 중단·복구 | 10건 발급, 중복·pending 0, 순서 유지 | [mysql-recovery](../4-coupon_experiments/results/mysql-recovery.json) |
| Redis 복제 지연 후 수동 승격 | 승인된 쓰기 손실 재현 | [redis-failover](../4-coupon_experiments/results/redis-failover.json) |
| 실제 쿠폰 예약 후 Redis 승격 | 예약 유실·재시도 후 불일치 재현, DB 총량 방어 확인 | [coupon-failover](../4-coupon_experiments/results/coupon-failover.json) |
| Sentinel 자동 승격 | 승격 및 복구 후 데이터 확인 통과 | [redis-sentinel](../4-coupon_experiments/results/redis-sentinel.json) |
| Redis Cluster 분포 | 18회, 실제 Lua 예약 216,000건 검증 통과 | [redis-cluster](../4-coupon_experiments/results/redis-cluster.json) |
| Lua 실행 오류 | 앞선 쓰기가 롤백되지 않는 반례 확인 | [lua-error-semantics](../4-coupon_experiments/results/lua-error-semantics.json) |
| Kafka 파티션 수 변경 | 동일 키의 기록 파티션 변경 확인 | [partition-growth](../4-coupon_experiments/results/partition-growth.json) |

**PASS의 의미를 구분해야 한다.** 정상 검증은 초과 발급·중복·원장 불일치가 없다는 관측이다. `expectedObservations`는 미발행 예약, 완료 순서 역전처럼 의도한 문제를 실제로 재현했다는 의미다. Redis 승인 쓰기 손실 실험의 PASS 역시 손실이 없다는 뜻이 아니라 손실 반례를 확인했다는 뜻이다. 예외 주입은 코드의 지정 위치에서 한 번 예외를 던지는 방식이며, 실제 노드 종료 실험과 구분한다. [실험 드라이버](../4-coupon_experiments/scripts/suite.py)

## 2. 환경과 측정 기준

| 항목 | 실제 구성 |
| --- | --- |
| 호스트·드라이버 | macOS 26.6.2 arm64, Python 3.14.4 |
| Docker | Server 29.5.3, 격리된 Compose 프로젝트 `coupon-lab` |
| MySQL | 8.0.46, DB `coupon_lab`, 포트 23306 |
| Redis | 7.4.9, primary 26379 / replica 26380, AOF `everysec` |
| Kafka | 3.8.1, KRaft 3노드, 업무 토픽 RF=3·minISR=2, Producer `acks=all`·idempotence. 본문 성능 측정은 heap 256MiB, 후속 복구 설정은 512MiB |
| Sentinel | 프로세스 3개, quorum=2 |
| Redis Cluster | master 3개, replica 0개, 16,384 슬롯 |
| 정상 HTTP 부하 | 재고 100, 요청 1,000, 클라이언트 동시 실행 32 |

이미지 digest·실제 버전은 [인프라 이벤트 기록](../4-coupon_experiments/results/infra/events.jsonl)에, 자원 제한과 포트는 [Compose](../4-coupon_experiments/compose.yml)에 남겼다. Kafka는 기존 예제의 3.7.1과 다른 3.8.1이다. 로컬에 있는 다른 프로세스와 CPU·디스크를 공유하므로 과거 벤치마크와 수치를 직접 합치지 않는다.

HTTP 응답 지연은 클라이언트의 요청 송신부터 응답 수신까지 측정한다. 최종 발급 지연은 `ISSUED` 요청의 `completedAt - receivedAt`이며, `receivedAt`은 DB 요청 원장 기록 시각이다. 여러 서버의 네트워크 도착 순서를 정의하는 전역 시계가 아니다. `202`와 `ISSUED`도 구분한다. 요청별 결과는 상태 API로 확인한다.

HTTP 실험의 p95는 nearest-rank로 계산하며, 발급 100건의 지연과 거절을 포함한 응답 1,000건의 지연은 표본이 다르다. 각 실행의 raw JSON에는 요청·응답, 관측 시각, 최종 상태, 검증 항목이 있다. 완료된 v2 요약에는 드라이버 및 JAR SHA-256도 포함된다. [측정 기준과 환경](../4-coupon_experiments/results/correctness-v2/summary.json)

## 3. 정상 발급과 요청 멱등성

**가설:** 다섯 경로가 같은 최종 DB 방어를 사용하면 정상 경쟁에서는 총량과 사용자별 발급 수를 지킬 수 있다.

**방법:** 각 경로에 대해 재고 100에 서로 다른 사용자 1,000명을 동시 실행 32개로 요청했다. 별도 실행에서 같은 사용자로 요청 ID를 달리한 64건, 같은 요청 ID의 동시 재시도 32건, 본문이 바뀐 요청 ID 재사용을 검증했다. 대소문자·악센트·후행 공백이 다른 식별자 3쌍, 총 6개도 검증했다.

**관측:** 다섯 경로 모두 100건 발급·900건 거절·미완료 0건이었다. 동일 사용자 64건은 한 건만 발급됐고, 같은 요청 ID의 재시도는 한 요청 기록을 유지했다. 이미 발급된 요청의 재호출은 기존 결과를 반환했으며, 같은 ID로 다른 사용자·쿠폰을 보내면 `409`였다. `Alice/alice`, `résumé/resume`, `user/user `는 서로 다른 식별자로 유지됐고 각 실험에서 6건이 발급됐다. [정상 검증 전체 증거](../4-coupon_experiments/results/correctness-v2/summary.json)

| 전략 | 발급 / 거절 / 미완료 | API 응답 p95, ms | 발급 완료 지연 p95, ms |
| --- | ---: | ---: | ---: |
| `SPLIT_REDIS` | 100 / 900 / 0 | 89.69 | 2,323 |
| `LUA_REDIS` | 100 / 900 / 0 | 33.06 | 1,316 |
| `KAFKA_FIRST` | 100 / 900 / 0 | 140.61 | 2,141 |
| `DB_OUTBOX` | 100 / 900 / 0 | 227.08 | 4,785 |
| `REDIS_STREAM` | 100 / 900 / 0 | 249.68 | 2,786 |

위 수치는 [포화 정상 검증 원본](../4-coupon_experiments/results/correctness-v2/)의 한 실행씩에서 관측한 값이다. 새 Consumer 그룹 기동과 실험 순서의 영향이 포함되며, 별도 성능 warmup을 둔 반복 측정이 아니다. Kafka 우선의 HTTP 응답은 `202` 1,000건이고, 거절 900건은 Consumer 처리 후 확정된다. 나머지 네 경로는 `202` 100건·즉시 거절 `409` 900건이므로 빠른 거절이 API 지연 표본 대부분을 차지한다. 이 표로 특정 경로가 운영에서 항상 빠르다는 결론을 내리지 않는다.

**한계와 기술적 고민:** 최종 안전성에 MySQL의 총량 조건과 유일 제약이 기여한다. Redis 예약만 안전하다고 가정하지 않았다. 식별자 비교 규칙도 정합성의 일부다. Redis가 구분하는 문자열을 DB가 같은 값으로 취급하면 서로 다른 예약이 최종 DB에서 충돌하므로 비교 규칙을 일치시키고 회귀 검증을 추가했다.

**큐 유입량:** 같은 포화 실험에서 `KAFKA_FIRST`는 1,000건을 Kafka에 발행한 뒤 900건을 거절했다. 나머지 네 경로는 예약에 성공한 100건만 발행했다. Kafka 앞단의 로그가 소진·중복 요청까지 받아 주는 대신 더 많은 메시지를 처리한다는 차이를 실제 요청의 `published` 상태와 offset으로 확인했다. 이는 재고 100인 이 부하의 관측이며 지속 처리 한계 측정과 구분한다.

## 4. 원자성, 예약과 전달 사이의 실패

### 4.1 분리 명령과 Lua의 보장 범위

**가설:** 개별 명령이 원자적이어도 여러 명령 사이 중단은 남는다. Lua 실행 중 오류도 DB 트랜잭션의 자동 롤백과 같지 않다.

**방법·관측:** `SPLIT_REDIS`에서 `SADD` 직후 예외를 발생시켰다. 재고는 10인데 사용자 집합은 1이고 발급은 0인 상태가 남았으며, 같은 사용자의 새 요청도 발급되지 않았다. [SADD 이후 중단](../4-coupon_experiments/results/faults-v2/001-fault-split-after-sadd.json)

별도 실제 Redis 스크립트는 같은 키에 `SET`한 뒤 `SADD`하여 `WRONGTYPE` 오류를 일으켰다. 오류 이후에도 `written-before-error` 값이 남았다. 즉 Lua의 연속 실행을 앞선 쓰기까지 되돌리는 롤백으로 해석할 수 없다. 이 반례는 일반 스크립트 의미를 검증한 것이며 쿠폰 Lua 내부의 모든 타입·메모리 오류를 전수 주입한 것은 아니다. [Lua 오류 원본](../4-coupon_experiments/results/lua-error-semantics.json), [재현 코드](../4-coupon_experiments/semantics-experiments.py)

### 4.2 예약 직후 중단한 요청은 누가 다시 발행하는가

| 경로 | 예약 직후 예외 이후 관측 | 복구 방식 |
| --- | --- | --- |
| `SPLIT_REDIS` | Redis 재고 9·사용자 1, 최종 발급 0, pending 1 | 같은 요청 ID를 명시적으로 재시도한 뒤 1건 발급 |
| `LUA_REDIS` | Redis 재고 9·사용자 1, 최종 발급 0, pending 1 | 같은 요청 ID를 명시적으로 재시도한 뒤 1건 발급 |
| `KAFKA_FIRST` | Consumer 예외 후 최종 1건 발급 | Kafka에 이미 기록된 요청을 다시 처리 |
| `DB_OUTBOX` | API 예외 후 최종 1건 발급 | 커밋된 Outbox를 relay가 처리 |
| `REDIS_STREAM` | API 예외 후 최종 1건 발급 | 예약과 함께 생성된 Stream 이벤트를 relay가 처리 |

**방법:** `AFTER_RESERVE`를 한 번 발생시키고 요청 상태·Redis 수량·최종 DB 발급을 조회했다. 앞의 두 경로는 자동 완료되지 않는 중간 상태를 먼저 확인한 뒤 같은 요청 ID로 재시도했다. 나머지는 명시적인 클라이언트 재요청 없이 완료되는지 확인했다. [예약·발행 실패 원본 묶음](../4-coupon_experiments/results/faults-v2/summary.json)

**한계:** 이 표는 지정 위치의 예외 주입 결과다. 프로세스 강제 종료는 별도 실험이며, Redis 복제본에도 예약과 Stream 이벤트가 반드시 남는다는 뜻은 아니다. Lua가 Redis 밖의 Kafka·DB까지 한 트랜잭션으로 묶는 것도 아니다. Stream relay는 단일 활성 소유자와 고정 consumer 이름을 사용한다.

### 4.3 전송 결과 불명, DB 커밋 이후 재처리, 상태 조회

| 장애·제어 | 실행한 방법 | 관측 |
| --- | --- | --- |
| Kafka 전송 ACK 직후 예외 | `AFTER_SEND` 후 같은 ID 재시도와 이벤트 재발행 | 최종 1건 유지, 중복 이벤트 처리 확인 |
| DB 커밋 후 offset 처리 전 예외 | `AFTER_DB` | 각 경로에서 재처리 카운터 증가, 최종 1건 유지 |
| 처리기 일시 정지 | Consumer·relay 정지 후 20건 접수 | 발급 0·pending 20, 재개 후 20건 완료 |
| handler 진입 전 metadata 조회 실패 | 해당 조회 위치의 일회성 예외 | 두 요청 모두 완료, 앞 요청을 재처리한 뒤 다음 요청 완료 |
| DB Outbox 예약 취소 | 발행 전 정지 상태에서 예약 후 취소 | 재고 10으로 복원, 재개 후에도 발급 0·취소 상태 유지 |

각 검증을 경로별로 실행했고, 취소는 DB Outbox에만 적용했다. [장애 주입 결과 27개](../4-coupon_experiments/results/faults-v2/summary.json)

**기술적 고민:** offset 관리만으로 DB 작업의 중복 효과를 막을 수 없으므로 DB 멱등성이 함께 필요하다. 반대로 DB가 안전해도 Kafka의 읽기 위치만 앞으로 간 채 실패한 레코드를 잊으면 처리가 멈출 수 있다. handler 이전 조회 실패와 오류 카운터 기록 실패까지 실제 요청의 재처리 경로를 끊지 않도록 다뤄야 한다. v2에는 metadata 조회 위치의 재시도·순서 회귀 검증을 포함했다. 실제 DB 서비스 중단·복구 결과는 11절에 기록했다.

## 5. API 도착, Kafka 기록, 실제 발급 순서

**가설:** Kafka 기록 순서와 API 원장 기록 순서는 달라질 수 있다. 같은 파티션에서도 뒤 요청을 먼저 처리하도록 허용하면 완료 순서는 달라진다.

| 실험 | 방법 | 실제 관측 |
| --- | --- | --- |
| API 기록 순서 vs Kafka 기록 순서 | 앞 요청의 전송에 700ms 지연 | 앞 요청이 원장에 먼저 있었지만 offset은 앞 요청 1·뒤 요청 0; 뒤 요청이 먼저 완료 |
| `BLOCKING` | offset 0 요청 처리에 1,000ms 지연 | offset 0·1의 완료 순번이 1·2 |
| `RETRY_LATER` | 같은 지연을 두고 뒤 요청 처리 허용 | offset 0·1의 완료 순번이 2·1 |
| 처리 중 Consumer 수 변경 | 6개 쿠폰·300건·3개 파티션, Consumer 1→3 | 300건 완료, 쿠폰별 50건, 각 쿠폰의 완료 순서에 따른 offset 오름차순 유지 |

원본은 [도착과 기록 순서](../4-coupon_experiments/results/ordering/001-ordering-arrival-versus-kafka-append.json), [BLOCKING](../4-coupon_experiments/results/ordering/002-ordering-delayed-first-blocking.json), [RETRY_LATER](../4-coupon_experiments/results/ordering/003-ordering-delayed-first-retry_later.json), [재할당 중 처리](../4-coupon_experiments/results/ordering/004-ordering-rebalance-while-processing.json)에 있다.

위 순서 실험에서는 명시적 지연을 넣어 역전 조건을 재현했다. 이를 자연적인 운영 지연 분포로 해석하지 않는다. Consumer 수 변경은 실험 API가 기존 worker를 중지하고 새 worker들을 시작해 그룹 재할당을 유발하는 절차다. 모든 리밸런싱 유형·분할 장애를 검증한 것은 아니다. 파티션 사이 완료 시각은 밀리초 해상도이고, 더 강한 비교는 쿠폰별 완료 순번과 해당 파티션 offset으로 수행했다.

`RETRY_LATER`는 완료 순서를 바꾸면서도 미완료 레코드를 넘어 offset을 커밋하지 않도록 [CommitBarrier](../4-coupon_experiments/spring/src/main/java/com/example/couponlab/CommitBarrier.java)를 사용한다. 따라서 “중복 효과 방지”, “안전한 offset 커밋”, “선착순 완료”는 서로 다른 요구사항으로 남는다.

### 파티션을 늘리면 기존 쿠폰 키는 어디로 가는가

**방법:** 실제 토픽의 파티션을 1개에서 6개로 늘렸다. `coupon-0`부터 `coupon-11`까지 같은 12개 키를 변경 전후에 발행하고, 새 파티션 정보를 읽는 새 Producer를 사용했다.

**관측:** 변경 전에는 12개 키 모두 파티션 0이었다. 변경 후 이 표본의 12개 키 모두 다른 파티션으로 매핑됐다. 예를 들어 `coupon-7`은 1, `coupon-9`는 2, `coupon-1`은 3, `coupon-0`은 4, `coupon-3`은 5에 기록됐다. 기존 레코드는 원래 파티션 0에 남았다. [실제 소비 결과 및 키별 매핑](../4-coupon_experiments/results/partition-growth.json)

**해석·한계:** 같은 키라는 이유만으로 파티션 수 변경 전후의 전체 요청이 계속 한 파티션에 있다고 가정할 수 없다. 이 결과는 해당 12개 키의 관측이며 모든 키가 반드시 이동한다는 뜻은 아니다. 소비 출력의 파티션 간 나열 순서도 전역 업무 순서를 뜻하지 않는다. 증설 중 기존 요청을 어떻게 비우고 새 매핑으로 전환할지 별도 정책이 필요하다.

## 6. Redis 장애와 자동 승격

### 승인된 쓰기가 승격된 복제본에 없는 경우

**가설:** primary의 성공 응답과 failover 후 데이터 보존은 같은 사건이 아니다.

**방법:** 먼저 baseline 값을 복제본까지 전달했다. 이후 이 실험의 복제본만 Docker 네트워크에서 분리하고 기존 복제 연결을 종료해 `connected_slaves=0`을 확인했다. primary에 새 값을 쓰고 `OK`를 받은 다음 primary를 SIGKILL했다. 네트워크가 분리된 복제본을 수동 승격한 뒤 값을 조회했다.

**관측:** baseline은 남았지만 방금 `OK`를 받은 새 값은 없었다. 이 결과는 복제 지연 중 승인된 쓰기 손실의 실제 반례다. [원본 결과](../4-coupon_experiments/results/redis-failover.json)

**한계:** 의도적으로 복제 연결을 끊은 수동 승격 실험이며 자연 장애의 손실 확률이나 일반적인 손실량을 측정한 것은 아니다. 개별 Redis 값의 보존을 확인한 실험이므로 쿠폰 재고·중복 집합·미완료 요청의 전체 재구축을 완료했다는 의미도 아니다. Docker Desktop의 네트워크 재연결 직후 포트 전달 지연과 Redis 상태를 구분하기 위해 증거 조회에는 해당 컨테이너의 `docker compose exec`를 사용했다.

### 실제 쿠폰 예약의 failover 불일치

**방법:** 재고 2개인 `LUA_REDIS` 경로의 초기 상태를 복제한 뒤 복제본을 격리했다. 실제 Lua 예약이 성공해 primary의 재고 1·사용자 `lost-user`·요청별 예약 순번을 확인했고, `AFTER_RESERVE`로 API에는 `503`을 반환하게 했다. 따라서 사용자에게 발급 성공을 응답한 사례가 아니라 **Redis 내부 예약은 승인됐지만 발행 전 중단된 사례**다. 이어 primary를 SIGKILL하고 복제본을 승격했다. [드라이버](../4-coupon_experiments/coupon-failover.py)

| 시점 | Redis 잔여 / 사용자 수 | DB 발급 / pending | 관측 |
| --- | ---: | ---: | --- |
| 예약 후, 종료 전 | 1 / 1 | 0 / 1 | Redis와 DB 요청 원장에 예약 있음 |
| 지연된 복제본 승격 | 2 / 0 | 0 / 1 | Redis 재고·사용자·요청별 예약 순번 유실 |
| 원래 주소 복구 후 같은 ID 재시도 | 2 / 0 | 1 / 0 | DB 발급은 진행되지만 Redis 예약은 자동 재구성되지 않음 |
| 서로 다른 사용자 2명 추가 요청 후 | 0 / 2 | 2 / 0 | 세 번째 요청은 `DB_CAPACITY_GUARD`로 거절 |

**관측:** 최종 DB 총량 제한이 초과 발급을 막았지만 Redis와 DB의 의미가 자동으로 같아지지는 않았다. 마지막 상태는 Redis 사용자 수와 DB 발급 수가 모두 2여도, Redis는 `additional-user-1`, `additional-user-2`이고 DB 발급 사용자는 `additional-user-1`, `lost-user`였다. 즉 **개수만 같다는 검사는 실제 사용자 집합의 불일치를 놓친다.** 검증 7개와 기대한 불일치 재현 3개가 모두 통과했다. [쿠폰 상태·집합 대조 원본](../4-coupon_experiments/results/coupon-failover.json)

**한계와 기술적 고민:** DB의 마지막 방어는 초과 발급을 막는 역할이며 Redis 상태를 고치는 기능이 아니다. 이번 구현에는 이 손실 이후 재고·사용자·미완료 예약을 모두 재구축하는 자동 정합성 복구가 없다. 운영에서는 신규 접수를 제한하고 확정 발급과 미완료 전달 기록까지 대조하는 복구 절차가 별도로 필요하다.

### Sentinel 자동 승격과 정상 토폴로지 복구

**방법:** Sentinel 3개가 서로를 발견하고 quorum 2를 구성한 뒤 primary를 실제 SIGKILL했다. 자동으로 replica가 master가 되는 시점을 기다리고, 승격 전 데이터와 승격 후 쓰기를 검증했다. 이후 승격된 노드를 정본으로 삼아 예전 primary를 동기화한 다음 원래 주소 역할로 복구했다.

**관측:** SIGKILL부터 master 역할 확인까지 **3.988초**였다. 사전에 복제된 값이 보존됐고 승격된 노드의 새 쓰기도 성공했으며, 원래 primary로 돌아온 뒤에도 그 새 값이 남았다. [Sentinel 원본](../4-coupon_experiments/results/redis-sentinel.json), [재현 스크립트](../4-coupon_experiments/infra/sentinel-experiments.py)

**한계:** 한 번의 로컬 장애 전환 시간이다. Spring 클라이언트의 Sentinel 주소 탐색·자동 연결 전환까지 검증한 값이 아니다. 앞의 손실 실험과 합쳐 보면 자동 승격이 된다는 사실과 복제되지 않은 쓰기가 보존된다는 주장을 구분해야 한다.

## 7. Redis Cluster의 hot key와 여러 쿠폰 분산

**가설:** 여러 쿠폰을 다른 master로 분산할 수 있지만, 한 쿠폰의 원자적 Lua 작업은 그 키들이 있는 하나의 슬롯 소유자에 남는다.

**방법:** 실제 master 3개를 항상 실행한 상태에서 같은 36개 쿠폰을 master 하나에 모은 경우와 세 master에 배분한 경우를 비교했다. 한 쿠폰 100% 집중, 약 90% 집중, 균등 분산을 각각 12,000건·동시 작업자 18개·3회 반복했다. persistent TCP RESP 연결 수와 Lua 작업은 동일하게 유지했고, 실행 순서를 고정 seed로 섞었다. 인위적인 처리 지연이나 CPU 작업은 넣지 않았다. [측정 코드](../4-coupon_experiments/infra/cluster-experiments.py)

| 분포 | 쿠폰 배치 | 처리량 중앙값, 명령/s | RTT p95의 중앙값, ms |
| --- | --- | ---: | ---: |
| 균등 | master 1개 | 15,262.52 | 2.1890 |
| 균등 | master 3개 | 16,794.25 | 1.9043 |
| 한 쿠폰 100% | master 1개 | 18,925.20 | 1.5760 |
| 한 쿠폰 100% | master 3개 | 16,900.43 | 1.7867 |
| 한 쿠폰 약 90% | master 1개 | 18,626.84 | 1.6670 |
| 한 쿠폰 약 90% | master 3개 | 18,871.99 | 1.6520 |

**관측:** 총 216,000건에서 각 실행의 사용자 수·잔여 재고·명령 호출 수가 모두 일치했다. 세 master에 배치한 경우의 master별 `EVALSHA` 증가량은 균등 `4,000 / 4,000 / 4,000`, 한 쿠폰 집중 `12,000 / 0 / 0`, 약 90% 집중 `11,228 / 391 / 381`이었다. 90% 쿠폰 외의 일부 쿠폰도 같은 master에 있으므로 해당 master의 실제 부하는 약 93.6%였다. 균등 분산 처리량은 이 표본에서 약 1.10배였으며, 단일 쿠폰은 다른 master를 사용하지 않았다. [측정값과 모든 반복 원본](../4-coupon_experiments/results/redis-cluster.json)

다른 슬롯의 두 키로 Lua를 호출하면 실제 `CROSSSLOT` 오류가 발생했다. 같은 hash tag를 가진 재고·사용자 키는 둘 다 슬롯 1607로 배치되어 Lua 예약이 성공했다. 따라서 원자적으로 함께 다루는 키들을 분산하는 문제와 서로 독립된 쿠폰을 분산하는 문제를 구분해야 한다.

**한계:** RTT는 Redis 명령 왕복이며 HTTP 발급 완료 지연이 아니다. 처리량 wall time에는 연결 생성과 worker 기동도 포함된다. 호스트 클라이언트가 공개 포트와 슬롯 소유자를 명시적으로 매핑했으며, 운영용 Cluster redirect 구성을 검증한 것은 아니다. 클라이언트·Docker 네트워크·공유 CPU가 먼저 병목일 수 있어 이 수치로 master 3개의 선형 성능 향상을 주장하지 않는다. replica가 없는 Cluster이므로 이 실험은 HA 검증에도 해당하지 않는다.

## 8. Kafka 분포와 Consumer 확장 135개 조합

**방법:** 전략 5개 × 쿠폰 균등/90%/100% 편중 × 파티션 1/3/6 × Consumer 1/3/6을 실행했다. 각 조합에서 20개 쿠폰의 warmup 20건을 완료한 뒤 측정 요청 300건을 동시 실행 32개로 보냈다. 재고는 충분히 두고 인위적인 처리 지연은 넣지 않았다. 실행 순서는 seed `20261022`로 섞었다.

**관측:** 135/135 케이스가 통과했다. 측정 40,500건과 warmup 2,700건이 모두 발급됐고, 원장·수량·중복 검증을 통과했다. 전체 실행은 약 717.6초였다. [전체 요약](../4-coupon_experiments/results/distribution/summary.json), [135개 비교 CSV](../4-coupon_experiments/results/distribution/comparison.csv), [압축 원본](../4-coupon_experiments/results/distribution/)

다음은 같은 **파티션 3개·Consumer 3개** 조건이다. 셀은 `API 응답 p95 / 최종 발급 p95`, 단위는 ms다.

| 전략 | 균등 20쿠폰 | 한 쿠폰 90% | 한 쿠폰 100% |
| --- | ---: | ---: | ---: |
| `SPLIT_REDIS` | 168.27 / 600 | 288.46 / 2,011 | 139.86 / 2,031 |
| `LUA_REDIS` | 183.71 / 1,111 | 107.89 / 1,257 | 148.98 / 1,220 |
| `KAFKA_FIRST` | 296.31 / 1,195 | 123.19 / 2,173 | 110.73 / 2,449 |
| `DB_OUTBOX` | 53.46 / 3,336 | 486.51 / 6,440 | 161.36 / 2,666 |
| `REDIS_STREAM` | 62.72 / 3,617 | 19.73 / 2,625 | 24.13 / 2,393 |

Kafka 우선 경로에서 **파티션을 6개로 고정하고 Consumer만 변경**한 결과는 다음과 같다. 완료 확인 처리량은 `300 / 클라이언트 제출 시작부터 전체 완료를 확인할 때까지의 초`이며 상태 조회 간격의 비용도 포함한다.

| Consumer 수 | 균등: 완료 확인 처리량, 건/s | 균등: 최종 p95, ms | 단일 쿠폰: 완료 확인 처리량, 건/s | 단일 쿠폰: 최종 p95, ms |
| --- | ---: | ---: | ---: | ---: |
| 1 | 98.92 | 2,272 | 114.15 | 1,924 |
| 3 | 117.04 | 1,478 | 52.90 | 4,596 |
| 6 | 181.48 | 785 | 91.82 | 2,689 |

균등 요청도 파티션별로 정확히 균등하지 않았다. 파티션 3개에는 `105 / 135 / 60`, 6개에는 `60 / 60 / 15 / 45 / 75 / 45`건이 들어갔다. 90% 편중을 3개 파티션으로 보내면 인기 쿠폰과 일부 다른 쿠폰이 충돌해 `280 / 13 / 7`건이었다. 단일 쿠폰 100%는 모든 조합에서 **측정 300건 모두 한 파티션**에 남았다. Consumer를 더 실행해도 그 쿠폰의 담당 Consumer 수는 늘지 않는다.

**한계와 기술적 고민:** 각 조합은 한 번의 짧은 부하이며 반복 측정의 신뢰구간이나 통계적 우열을 제공하지 않는다. 단일 쿠폰 표의 큰 변동을 특정 설정의 본질적인 성능 차이로 단정하지 않는다. 모든 경로에 동기식 SQL 요청 원장, 최종 쿠폰 행 잠금, 상태 조회 비용이 있고 Outbox·Stream에는 relay 처리도 포함된다. 따라서 end-to-end 처리량 차이만으로 Kafka나 Redis CPU 포화를 원인으로 확정할 수 없다. 라우팅 자료는 집중 위치를 증명하고, 별도 Redis Cluster 실험은 노드별 명령 분포를 확인한다. 운영 파티션 수는 예상 쿠폰 분포·최대 편중·소비자 처리 시간·장애 복구 여유를 함께 측정해 정해야 한다.

## 9. API JVM 1개와 2개

**방법:** `LUA_REDIS`의 단일 쿠폰·20개 균등 쿠폰 × JVM 1개·2개 × 2회 반복, 총 8회를 순서를 섞어 실행했다. 각 실행은 warmup 20건 후 측정 1,000건, 동시 실행 32개다. 두 JVM일 때 클라이언트가 두 실제 HTTP 주소에 번갈아 요청했다. JVM마다 같은 JAR와 heap 설정을 사용했다. [드라이버](../4-coupon_experiments/api-scaling.py)

접수 경로만 비교하기 위해 측정 중 Consumer를 정지했다. 두 번째 JVM을 종료한 뒤 주 JVM의 Consumer 3개로 전부 발급하고 독립 DB 원장을 확인했다. 따라서 아래 처리량은 Redis 예약·공통 DB 기록·Kafka ACK까지의 HTTP 접수 처리량이며, 최종 발급까지의 확장 효과가 아니다.

| 분포 | JVM 1개: 접수 건/s | JVM 2개: 접수 건/s | 비율 | JVM 1개: API p95, ms | JVM 2개: API p95, ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| 단일 쿠폰 | 332.89 | 406.63 | 1.22배 | 281.77 | 192.32 |
| 20쿠폰 균등 | 337.27 | 271.92 | 0.81배 | 230.75 | 279.70 |

**관측:** 표는 각 조건의 두 실행 값의 중앙값이며, p95 열도 실행별 p95의 중앙값이다. 측정 8,000건은 모두 `202`였고 warmup을 포함한 8,160건이 DB에서 각각 한 번 발급됐다. 모든 실행에서 pending 0·중복 0·총량 대조를 확인했다. 추가 JVM 4회는 각각 종료됐고 측정 포트도 닫혔다. [응답·프로세스·DB 검증 원본](../4-coupon_experiments/results/api-scaling.json)

**한계와 기술적 고민:** 단일 머신의 두 JVM은 CPU·디스크·MySQL·Redis·Kafka를 공유한다. DB 연결 풀과 Producer도 JVM 수에 따라 늘고, 뒤늦게 시작한 JVM의 JIT 상태가 다르다. warmup 20건과 반복 2회는 작아 균등 부하의 감소 원인이나 Redis 포화 여부를 확정할 수 없다. 이 실험에서 증설 효과가 일정하지 않았다는 사실을 남기며, 독립 호스트·충분한 예열·장시간 반복과 서버별 자원 측정을 후속으로 둔다.

실행을 만든 뒤 두 번째 JVM을 시작하고 완료 검증은 주 JVM에서 수행한다. 운영용 서비스 검색, 새 실행의 실시간 노드 간 전파, 분산 제어, relay 리더 선출은 구현·검증 범위에 포함하지 않는다.

## 10. JVM 재시작과 발급·취소 경쟁

**방법:** 다섯 경로에 미완료 요청 한 개씩을 만들고 Consumer·relay를 일시 정지했다. Kafka 우선은 ACK를 받은 요청, 나머지는 예약 직후 예외를 발생시킨 요청이다. 실제 JVM을 SIGKILL한 뒤 같은 JAR를 다시 시작했다. 재시작 직후에도 `paused=true`, worker 1개, 발급 0건인 상태를 검증하고 명시적으로 재개했다. [드라이버](../4-coupon_experiments/restart-experiments.py)

| 전략 | 재개 후 자동 발급 / pending | 이후 확인 |
| --- | ---: | --- |
| `SPLIT_REDIS` | 0 / 1 | 같은 ID로 재요청한 뒤 1건 발급 |
| `LUA_REDIS` | 0 / 1 | 같은 ID로 재요청한 뒤 1건 발급 |
| `KAFKA_FIRST` | 1 / 0 | 클라이언트 재요청 없이 완료 |
| `DB_OUTBOX` | 1 / 0 | Outbox에서 자동 재개 |
| `REDIS_STREAM` | 1 / 0 | 실제 PEL의 미확인 이벤트 복구 |

Stream은 종료 전에 `XREADGROUP`으로 `lab-relay` consumer에 한 이벤트를 전달하고 ACK하지 않았다. `XPENDING=1`을 확인한 뒤 종료했으며, 재시작 후 같은 consumer가 PEL을 읽고 발급한 뒤 `XPENDING=0`이 됐다. 새 backlog만 처리한 실험과 구분된다. 다섯 경우 모두 최종 DB 원장 1건·중복 0건·미완료 0건을 확인했다. [프로세스 복구 원본](../4-coupon_experiments/results/process-recovery.json)

**발급·취소 경쟁:** DB Outbox에 60건을 접수하며 12개 작업자가 취소를 요청했다. 절반에는 취소 요청 전 600ms를 두어 발급 확정과 경쟁시켰다. 최종 실행에서 **발급 30·취소 30**이었다. 성공한 취소는 모두 `CANCELLED`로 유지됐고, 이미 발급된 요청의 취소는 `409`였다. `잔여 30 + 발급 30 = 총량 60`, 독립 원장의 중복 0을 확인했다. 취소와 발급이 같은 요청 행을 잠그는 트랜잭션 경계가 이 경쟁의 방어다.

**실험 도구 수정 기록:** 최초 두 시도에서는 SIGKILL 후 종료 중인 프로세스의 명령 표시가 바뀌어 PID 소유권 검사에서 중단됐다. 신호 전 소유권 검사는 유지하고, 신호를 보낸 뒤 종료 대기에서는 원래 JVM이 사라진 것으로 처리하도록 수정했다. 이후 전체 실험이 통과했다. [첫 실패와 취소 결과](../4-coupon_experiments/results/process-recovery-initial-harness-failure.json), [두 번째 실패와 취소 결과](../4-coupon_experiments/results/process-recovery-second-harness-failure.json)를 보존한다. 두 번째 시도의 취소 60건은 전부 취소가 이겼으므로 최종 30/30 관측과 합쳐 쓰지 않았다.

**한계:** 같은 호스트의 JVM 재시작이며 DB·Redis·Kafka의 동시 유실을 검증하지 않는다. Stream relay는 하나의 활성 소유자가 같은 이름으로 돌아오는 조건이다. 다중 relay 간 소유권 이전이나 `XAUTOCLAIM` 경쟁은 후속 과제다.

## 11. 실제 Kafka·DB 서비스 장애

### Kafka 리더 SIGKILL: 승인된 대기 요청과 장애 중 새 요청

Kafka 우선 경로에서 Consumer를 일시 정지하고 요청 한 건의 ACK를 받았다. 발급 0·pending 1·`published=true`를 확인한 뒤, 실제 metadata에서 해당 파티션의 leader인 `kafka2`를 찾아 SIGKILL했다. Consumer를 재개하고 새 사용자 40건을 동시 실행 12개로 요청했다.

새 요청은 **`202` 28건·`503` 12건**이었다. 브로커를 복구한 뒤 실패한 12건을 같은 요청 ID로 재시도했고, 장애 전 대기 요청까지 **DB 발급 41건·고유 사용자 41명·pending 0건**으로 완료됐다. 이 사례는 ACK된 미소비 요청의 보존과 장애 중 불확실한 요청의 재시도를 함께 확인한다. 실패 응답을 받았다는 이유로 새 요청 ID를 발급하지 않았다. [리더 장애 원본](../4-coupon_experiments/results/kafka-leader-failover.json), [드라이버](../4-coupon_experiments/ha-experiments.py)

### Kafka 2노드 중단: Redis 예약 뒤 실제 발행 실패

**방법:** 정상 상태에서 다섯 경로의 토픽·실행을 만들고 실제 토픽의 RF 3·minISR 2를 조회했다. `kafka2`, `kafka3`을 중단한 상태에서 각 경로에 한 건씩 요청했다. 요청 원장과 Redis/Outbox/Stream을 조회한 뒤 브로커를 복구했다. [드라이버](../4-coupon_experiments/kafka-outage-paths.py)

| 경로 | 중단 중 HTTP | 중단 중 남은 기록 | 복구 후 관측 |
| --- | --- | --- | --- |
| `SPLIT_REDIS` | 503 | Redis 잔여 4·사용자 1, pending 1 | 자동 완료 0, 같은 ID 재시도로 1건 발급 |
| `LUA_REDIS` | 503 | Redis 잔여 4·사용자 1, pending 1 | 자동 완료 0, 같은 ID 재시도로 1건 발급 |
| `KAFKA_FIRST` | 503 | Redis 잔여 5·사용자 0, 요청 원장 pending 1 | 자동 완료 0, 같은 ID 재시도로 1건 발급 |
| `DB_OUTBOX` | 202 | DB 잔여 4·활성 예약 1·Outbox 1 | 클라이언트 재요청 없이 1건 발급 |
| `REDIS_STREAM` | 202 | Redis 잔여 4·사용자 1·Stream 대기 1 | 클라이언트 재요청 없이 1건 발급 |

다섯 경우 모두 최종 발급 원장 1건·중복 0건·pending 0건이고 relay의 대기 항목도 비었다. Lua로 예약을 묶어도 **실제 Kafka 중단 중 재고는 이미 차감된 채 발행이 실패할 수 있음**을 확인했다. Outbox·Stream은 예약과 함께 남긴 전달 기록으로 다시 발행했다. [다섯 경로 원본](../4-coupon_experiments/results/kafka-outage-paths.json)

이 실행의 직접 전송 세 경로는 재시도 전 발급 0건이었지만, 모든 Producer timeout이 미기록을 뜻하는 것은 아니다. 드라이버는 복구 후의 실제 상태를 기록하고 같은 ID를 재사용한다. 또한 broker와 controller 역할을 세 노드가 함께 수행하므로 두 노드 종료는 controller 과반수와 ISR을 함께 잃는 조건이다. `min.insync.replicas`만 독립적으로 바꾼 실험이라고 설명하지 않는다.

### MySQL 중단: 처리 중 요청을 잊지 않고 다시 실행

Kafka 우선 경로에 10건을 기록하고, 첫 요청에 처리 지연 1,000ms를 둔 뒤 Consumer를 재개하면서 MySQL 서비스를 실제 중단했다. 중단 중 상태 조회는 `503`이었다. 서비스를 다시 기동하자 **10건 모두 발급·독립 원장 10건·중복 0·pending 0**으로 완료됐다. offset으로 정렬한 쿠폰별 완료 순번도 오름차순이었다. [DB 중단 원본](../4-coupon_experiments/results/mysql-recovery.json)

이 실험은 실제 DB 가용성 장애 후 재처리를 검증한다. 정확히 handler 이전 조회 위치만 분리한 실험은 아니며, 해당 위치의 단위 테스트와 일회성 오류 주입 검증이 이를 보완한다. DB 중단 후 재기동은 복제 MySQL의 자동 failover와 다르다. 장시간 장애로 `max.poll.interval.ms`를 넘기거나 네트워크 분할 중 오래된 Consumer가 계속 쓰는 경우까지 검증하지 않았다.

## 12. 기존 구현 재검증과 재현 방법

기존 Phase 1~3의 두 동시성 테스트를 격리된 이번 인프라에 연결해 재실행했다. 각 단계에서 “1,000명 요청에 정확히 100개”, “동일 사용자 100회 요청에 한 개”가 최종 통과했다. JUnit 실행 시간은 테스트 환경 준비·기다림을 포함할 수 있으므로 이번 보고서의 HTTP 처리량으로 사용하지 않았다. [기존 구현 테스트 원본](../4-coupon_experiments/results/original-baselines.json)

Phase 3 첫 실행은 자동 토픽 생성이 꺼진 새 클러스터에서 `coupon-issue`가 없는 상태로 시작해 테스트 하나가 실패했다. 토픽을 1개 파티션·RF 3·minISR 2로 준비한 뒤 두 테스트가 통과했다. 초기 실패와 재실행을 같은 JSON에 보존했으며, 성공한 최종 실행만 남기지 않았다. 기존 2026-09-12의 k6 수치는 [과거 벤치마크](benchmark-2026-09-12.md)로 분리한다.

### 최종 검토에서 보강한 종료 처리와 실험 환경

Consumer 교체 시 기존 worker 목록을 먼저 제거하면, 종료 대기가 시간 초과됐을 때 다음 제어 요청이 아직 살아 있는 worker를 추적하지 못할 수 있었다. 최종 소스는 종료가 확인될 때까지 기존 목록을 유지하도록 보강했다. 본문의 186개 비교 케이스·API 확장·HA 측정은 JAR `f71c55b9…`에서 수행했다. 당시 소스는 [측정 소스 보관본](../4-coupon_experiments/results/measured-backend-source.zip)에 보존했다. 최종 JAR `76e2f4b2…`와의 실행 코드 차이는 worker 목록 조회의 `remove → get`이며, 종료 실패 상태를 유지하기 위한 변경이다. 장시간 정지 I/O와 모든 Consumer fencing 조건을 해결했다는 뜻은 아니다.

최종 재검증 도중에는 Kafka 1의 Java heap 부족을 발견했다. 모든 본문 측정을 마친 뒤인 **10:09:59 UTC**부터 브로커 로그에 `OutOfMemoryError`가 나타났고, 누적된 실험 토픽은 264개였다. API 버전 조회만 사용하는 healthcheck는 정상으로 표시됐지만 Raft 등 내부 스레드가 멈췄고 새 토픽을 보지 못하는 Consumer가 생겼다. 해당 순서 검증은 [인프라 장애 당시 결과](../4-coupon_experiments/results/ordering-final-infra-failure/summary.json)에 실패 2·통과 2로 남겼다. 리밸런싱 케이스는 환경 복구를 위해 실행을 중단했으므로 애플리케이션 순서 결함으로 단정하지 않는다.

당시 설정 [heap 256MiB·메모리 한도 768MiB](../4-coupon_experiments/results/compose-measured.yml)과 [장애 로그·복구 증거](../4-coupon_experiments/results/kafka-heap-recovery.json)를 보존했다. 이후 heap 512MiB·메모리 한도 1GiB로 늘리고 `-XX:+ExitOnOutOfMemoryError`로 부분 정지 상태를 줄이도록 설정했다. 볼륨을 유지한 채 브로커를 하나씩 교체하고 ISR 복구를 확인했다. 앞의 성능 표는 이 증설 이후에 다시 측정한 값이 아니며, 긴 실험에서 토픽·그룹 보존과 메모리 여유도 관리해야 한다는 한계를 추가한다.

보강 후 최종 JAR와 복구된 환경에서 **순서·리밸런싱 4/4**, **다섯 경로의 실제 JVM 재시작과 발급·취소 경쟁**을 다시 실행해 통과했다. 단위 테스트 5개도 통과했다. [최종 순서 재검증](../4-coupon_experiments/results/ordering-final/summary.json), [최종 프로세스 재검증](../4-coupon_experiments/results/process-recovery-final.json), [원본 JSON·압축 데이터·소스 해시·검증 기록](../4-coupon_experiments/results/verification.json)에서 당시 측정과 최종 검증을 구분할 수 있다.

최종 취소 재검증에서는 60건 모두 취소가 먼저 완료됐다. 따라서 10절의 발급 30·취소 30이라는 양쪽 승리 관측은 이전 실행의 결과로 유지하며, 최종 재검증에서 같은 비율이 재현됐다고 주장하지 않는다.

```bash
cd 4-coupon_experiments
python3 infra/lab.py up
python3 app.py start --build
python3 scripts/suite.py --scenario correctness --output results/reproduction-correctness
python3 scripts/suite.py --scenario faults --output results/reproduction-faults
python3 scripts/suite.py --scenario ordering --output results/reproduction-ordering
python3 scripts/suite.py --scenario distribution --gzip-cases --output results/reproduction-distribution
python3 api-scaling.py --output results/reproduction-api-scaling.json
python3 restart-experiments.py --output results/reproduction-process.json
python3 semantics-experiments.py

# 장애 실험과 일반 부하를 겹치지 않게 실행한다.
python3 ha-experiments.py --scenario redis --output results/reproduction-redis-failover.json
python3 ha-experiments.py --scenario kafka --output results/reproduction-kafka-leader.json
python3 ha-experiments.py --scenario db --output results/reproduction-mysql.json
python3 kafka-outage-paths.py --output results/reproduction-kafka-paths.json
python3 coupon-failover.py --output results/reproduction-coupon-failover.json
python3 infra/sentinel-experiments.py
python3 infra/lab.py cluster-up
python3 infra/cluster-experiments.py
```

노드 제어는 `coupon-lab`으로 제한하며 기존 프로젝트의 컨테이너·볼륨을 사용하지 않는다. `infra/lab.py down`은 실험 컨테이너를 내리고 볼륨을 보존한다. 상세 명령과 역할 복구 절차는 [인프라 안내](../4-coupon_experiments/infra/README.md)에 있다.

## 13. 실험으로 좁혀진 기술적 선택

정상 경쟁의 최종 수량이 맞더라도 예약과 발행의 연결이 끊기면 pending 요청이 남는다. 접수 결과를 사용자에게 돌려주려면 재시작 후 다시 진행할 근거와 요청별 상태 조회가 함께 필요하다. Kafka 우선은 모든 접수를 로그에 남기는 비용을, DB Outbox는 예약·전달 기록을 같은 트랜잭션에 넣는 비용을, Redis Stream은 Redis 상태의 내구성과 relay 소유권을 각각 검토해야 한다.

엄격한 선착순을 요구하면 API 원장 기록, Kafka offset, Redis 예약 순번, 최종 완료 순번 중 어떤 기준을 약속할지 먼저 정해야 한다. 뒤 요청을 허용하는 재시도 방식은 완료 순서를 바꿀 수 있으며, 파티션 수 변경도 키 매핑의 연속성을 별도로 다뤄야 한다.

여러 쿠폰을 분산하는 것과 한 쿠폰을 여러 노드에 나누는 것은 다른 설계다. 이번 구현은 전자를 실측했고, 후자의 재고 버킷·중복 판정·엄격한 순위 병합은 구현하지 않았다. Redis failover 이후 전체 예약을 재구축하는 운영 절차, 다중 relay의 소유권, 모든 장애 조합의 무손실 보장은 남은 범위다. 후속 논의는 [멘토링 질문](mentoring-discussion.md)과 연결한다.

실험 서버는 유한한 실행을 생성하고 끝날 때 `paused=true, workers=0`으로 종료하는 도구다. worker 상한 32는 실행별 상한이며 전역 제한이 아니다. 요청별 잠금·실행 정보, Redis 중복 기록·Stream, Kafka 토픽의 보존·삭제 정책과 장기 장애 중 `RETRY_LATER` 대기 목록의 상한도 구현하지 않았다. 운영에 사용하려면 전역 자원 제한, 대기열 상한, 보존 기간, 장시간 I/O 정지와 오래된 Consumer의 쓰기 차단을 추가로 검증해야 한다.

## 14. 세션 복구와 후속 검증

원본 작업 폴더에 남아 있던 미커밋 파일 321개를 `coupon-session-recovery` 브랜치로 복구했다. 복사한 파일의 SHA-256을 모두 대조했고 원본은 변경하지 않았다. `runtime/`의 PID 파일과 빌드 산출물은 복사에서 제외했다. 복구 직후 새 checkout에서 빌드한 JAR의 SHA-256은 앞선 최종 JAR `76e2f4b2…`와 같았다. 기존 측정 결과는 보존하고 이후 실행은 [별도 결과 디렉터리](../4-coupon_experiments/results/recovery-2026-10-22/)에 기록했다.

### 요청 재시도와 실행 도구의 경계 보강

`DB_OUTBOX`의 `submit()`은 예약 트랜잭션에 들어가기 전에 요청 상태를 읽는다. 여러 API 인스턴스가 같은 요청의 이전 상태를 읽으면 한쪽이 종료한 뒤 다른 쪽이 예약 행 잠금을 얻을 수 있다. 예를 들어 재고 부족으로 `REJECTED`가 된 후 다른 예약 취소로 재고가 생기면, 이전 코드는 거절된 요청을 다시 `RESERVED`로 바꿀 수 있었다. 요청 행을 `FOR UPDATE`로 읽은 직후 `ISSUED`·`REJECTED`·`CANCELLED`를 다시 확인하고 기존 결과를 반환하도록 수정했다.

[LabStoreTest](../4-coupon_experiments/spring/src/test/java/com/example/couponlab/LabStoreTest.java)는 JDBC를 모킹해 잠금을 얻었을 때 이미 종료된 요청을 반환한다. 세 종료 상태 모두 기존 코드에서 실패했고 가드 추가 후 통과했다. 진행 중인 예약을 재사용하며 재고를 중복 차감하지 않는 테스트도 포함한다. 이 회귀 검증은 실제 다중 JVM 경쟁 부하를 실행한 결과와 구분한다.

실행 도구는 복사된 PID 파일이 다른 checkout의 JAR을 가리키면 종료 신호를 보내지 않도록 보강했다. HTTP 포트가 이미 점유됐으면 JVM을 생성하기 전에 실패시켜 기존 서버의 health 응답을 새 서버의 기동 성공으로 오인하는 경우도 차단했다. [Python 회귀 테스트](../4-coupon_experiments/tests/test_app.py)는 임시 상태파일, 실제 임시 로컬 포트, 프로세스 호출 모킹으로 검증한다. 인프라의 `coupon-lab` 이름과 포트는 checkout 간 공유하므로 동시 실행은 지원하지 않는다.

### 복구 후 재실행한 범위

| 검증 | 결과 | 근거 |
| --- | --- | --- |
| Java 전체 단위 테스트와 `bootJar` | 9/9 통과, 빌드 성공 | [복구 검증 기록](../4-coupon_experiments/results/recovery-2026-10-22/verification.json) |
| Python 프로세스 제어 회귀 테스트 | 3/3 통과 | 같은 검증 기록 |
| 정상·중복·요청 멱등성·식별자 경계 | 20/20 통과 | [정상 검증](../4-coupon_experiments/results/recovery-2026-10-22/correctness/summary.json) |
| 장애 주입·재처리·일시 정지·취소 | 27/27 통과 | [장애 검증](../4-coupon_experiments/results/recovery-2026-10-22/faults/summary.json) |
| 처리 순서·Consumer 재할당 | 4/4 통과 | [순서 검증](../4-coupon_experiments/results/recovery-2026-10-22/ordering/summary.json) |
| 실제 JVM SIGKILL 후 재기동 | 다섯 경로 모두 기대한 복구·명시적 재시도 결과 확인 | [재시작 검증](../4-coupon_experiments/results/recovery-2026-10-22/process-recovery.json) |
| DB Outbox 발급·취소 경쟁 60건 | 발급 20·취소 40, 미완료·중복 0 | 같은 재시작 검증 기록 |

이 재실행은 변경 후의 기능 회귀 확인이다. 앞선 135개 분포·확장 성능 비교나 Redis/Kafka 노드 failover 실험을 새 JAR로 모두 반복한 것은 아니며, 본문의 성능 표와 장애 관측은 당시 실행 결과로 유지한다.
