# 선착순 쿠폰 설계 비교 실험

다섯 가지 발급 경로를 같은 Spring Boot 애플리케이션에서 구현하고 실제 MySQL·Redis·Kafka에 연결해 비교한다. [멘토링 논의안](../docs/mentoring-discussion.md)의 정상 발급, 원자성, 전달 실패, 중복 재처리, 순서, 편중 트래픽, 장애 복구를 재현하기 위한 독립 실험 모듈이다.

측정 결과와 해석은 [실증 보고서](../docs/experiments-2026-10-22.md), 요청·응답과 최종 상태의 원본은 [results](results/)에 기록한다. 기존 Phase 1~3 구현과 과거 벤치마크는 별도로 유지한다.

## 구현한 경로

| 전략 | 재고 예약·전달 | 의도적으로 비교하는 특성 |
| --- | --- | --- |
| `SPLIT_REDIS` | 개별 `SADD → DECR` 후 Kafka | 명령 사이 중단으로 사용자 정보만 남는 기준선 |
| `LUA_REDIS` | Lua 중복·재고·요청 ID 판정 후 Kafka | Redis 예약은 원자적이지만 예약과 발행 사이 자동 복구 기록은 없음 |
| `KAFKA_FIRST` | Kafka ACK 후 Consumer에서 Lua 판정 | 중복·소진을 포함한 모든 접수 요청이 로그를 거쳐 최종 결과를 받음 |
| `DB_OUTBOX` | DB 예약·총량 차감·Outbox를 한 트랜잭션으로 기록 | relay 재시도와 Consumer 멱등성으로 예약 이후 발행 실패 복구 |
| `REDIS_STREAM` | 같은 슬롯의 Lua에서 예약·`XADD` 실행 | relay가 Kafka ACK 후 `XACK`; 재시작 시 pending 목록부터 복구 |

**모든 경로의 공통 조건:** MySQL 총량 제한과 `(runId, couponId, userId)` 유일 제약이 있고, 발급 행·결과 상태를 같은 최종 DB 트랜잭션에서 확정한다. DB Outbox는 앞선 예약 트랜잭션에서 재고를 차감하며 최종 발급 시 중복 차감하지 않는다. 나머지는 최종 발급 트랜잭션에서 DB 재고도 차감한다. 같은 요청 ID에 다른 `couponId`·`userId`를 보내면 `409`로 거절한다. 실험용 지연·오류 옵션은 최초 기록을 재사용한다. 식별자 비교는 Redis와 동일하게 대소문자·악센트를 구분한다.

실험의 요청 추적과 재시도 본문 검증을 위해 **모든 API 경로가 동기식 DB 요청 원장을 먼저 기록**한다. Kafka 메시지는 그 요청 ID를 참조한다. 따라서 이 구현은 DB 접근이 전혀 없는 Redis/Kafka 앞단의 절대 성능을 측정하지 않는다. 기존 Phase 3의 fire-and-forget과 달리 직접 Kafka 전송 경로도 ACK를 확인한다. 이 공통 비용과 보장 조건을 유지한 상태에서 처리 위치·복구 방식의 차이를 비교한다.

DB 요청 원장은 관측·식별에 사용한다. `SPLIT_REDIS`와 `LUA_REDIS`의 미발행 예약을 자동으로 스캔·발행하지 않으므로 자동 복구가 있는 Outbox/Stream과 구별할 수 있다. 미발행 예약은 같은 요청 ID의 명시적 재시도로 이어서 처리할 수 있다.

## 실행

요구 환경은 JDK 17 이상, Docker Compose, Python 3다. Java 코드는 17 toolchain으로 빌드하고 실험 드라이버는 Python 표준 라이브러리만 사용한다. 모든 포트는 localhost에 바인딩하며 Compose 프로젝트 이름은 `coupon-lab`이다. 실험용 계정 `coupon/coupon`을 사용한다.

```bash
cd 4-coupon_experiments
python3 infra/lab.py up
python3 app.py start --build

# 정상·중복·장애 주입·순서·분포 전체 비교
python3 scripts/suite.py --scenario all --gzip-cases --output results/reproduction

# 실제 JVM SIGKILL 후 재시작 및 발급/취소 경쟁
python3 restart-experiments.py --output results/reproduction-process.json

# 실제 Redis·Kafka 노드 장애; 부하 실험과 동시에 실행하지 않는다
python3 ha-experiments.py --scenario all --output results/reproduction-ha.json
python3 kafka-outage-paths.py --output results/reproduction-kafka-paths.json
python3 coupon-failover.py --output results/reproduction-coupon-failover.json
python3 infra/sentinel-experiments.py

# Redis Cluster 슬롯과 편중 부하 비교
python3 infra/lab.py cluster-up
python3 infra/cluster-experiments.py

# 동일 부하에서 API JVM 1개와 2개 비교
python3 api-scaling.py

# Lua 오류의 부분 쓰기, 실제 Kafka 파티션 증설 전후 키 이동
python3 semantics-experiments.py
```

특정 시나리오만 실행하려면 `scripts/suite.py --help`를 참고한다. 기본 정상 부하는 재고 100개에 사용자 1,000명, 동시 실행 32개다. 분포 비교는 파티션 수 `1/3/6` × Consumer 수 `1/3/6` × 균등/90% 편중/단일 쿠폰 × 전략 5개이며, 각 실행 전에 warmup 요청을 완료한다.

`app.py crash`는 저장된 JAR가 현재 checkout의 빌드 디렉터리에 속하는지와 PID의 실제 명령을 확인한 뒤 해당 JVM만 종료한다. `start`는 HTTP 포트가 이미 점유됐으면 새 JVM을 실행하기 전에 실패한다. 다른 checkout으로 옮길 때 `runtime/`의 PID 파일은 복사하지 않는다.

인프라 제어는 `coupon-lab` 프로젝트로 한정한다. 같은 저장소의 여러 checkout도 이 프로젝트 이름·포트·볼륨을 공유하므로 한 번에 하나만 실행한다. 일반 종료는 다음과 같으며 볼륨을 삭제하지 않는다.

```bash
python3 app.py stop
python3 infra/lab.py down
```

인프라 없이 실행하는 회귀 테스트는 다음과 같다. Java 테스트는 완료·거절·취소된 Outbox 요청의 재예약 차단을, Python 테스트는 프로세스 소유권과 포트 충돌 처리를 포함한다.

```bash
cd spring
./gradlew test bootJar --no-daemon
cd ..
python3 -m unittest discover -s tests -v
```

재검증은 `--output`을 새 경로로 지정하고, 인프라 기록은 `COUPON_LAB_EVIDENCE`로 분리한다. 세션 복구 후의 검증·수정 내역은 [실증 보고서 14절](../docs/experiments-2026-10-22.md#14-세션-복구와-후속-검증)에 있다.

## API와 실험 제어

| 요청 | 용도 |
| --- | --- |
| `POST /lab/runs` | 전략·총량·쿠폰 수·파티션 수·Consumer 수를 지정한 독립 실행 생성 |
| `POST /lab/runs/{runId}/requests` | `requestId`, `couponId`, `userId`로 접수 |
| `GET /lab/runs/{runId}/requests/{requestId}` | 요청별 처리 중·발급·거절·취소 조회 |
| `GET /lab/runs/{runId}` | Redis 재고/사용자 수, DB 발급/잔여량, 요청별 순번·offset·시각 대조 |
| `POST /lab/runs/{runId}/control` | Consumer·relay 일시 정지, Consumer 수 변경으로 재할당 유발 |
| `POST /lab/runs/{runId}/replay` | 이미 존재하는 요청의 Kafka 이벤트 재발행 |
| `POST /lab/runs/{runId}/cancel` | DB Outbox 예약 취소; 발급 확정과 같은 DB 행 잠금으로 경쟁 |

`202`는 접수 상태다. 완료 결과는 상태 API에서 확인한다. 각 실행을 마치면 `{ "paused": true, "workers": 0 }`으로 처리기를 정지해 이전 실행이 다음 측정의 부하가 되지 않게 한다.

`fault`는 `AFTER_ADD`, `AFTER_RESERVE`, `AFTER_SEND`, `AFTER_DB`, `READ_BEFORE_HANDLER` 중 하나다. 지정한 위치에서 한 번 예외를 발생시키며, 실제 JVM 종료와 별도의 실험이다. `AFTER_ADD`는 분리 명령 기준선에만 적용한다. `READ_BEFORE_HANDLER`는 Consumer의 요청 조회 실패 후 같은 레코드를 재처리하는 경로를 검증한다. `publishDelayMs`와 `processingDelayMs`는 순서가 달라지는 상황을 결정적으로 재현할 때만 사용한다.

`BLOCKING`은 앞 요청의 처리 완료를 기다리고, `RETRY_LATER`는 뒤 요청 처리를 허용하되 미완료 offset을 넘어 커밋하지 않는다. 완료 순번은 쿠폰별 순번이다. API의 `receivedAt`은 DB 요청 기록 시각이며 여러 서버의 네트워크 도착 순서에 대한 절대 기준은 아니다.

## 측정과 한계

- 요청 ID별 HTTP 응답 시간, 최종 발급까지의 시간, 발급·거절을 포함한 최종 결과까지의 시간을 분리한다. p95는 nearest-rank 방식이다.
- raw case 파일은 `.json` 또는 `.json.gz`, 비교 요약은 `summary.json`이다. 의도한 실패 재현은 `expectedObservations`, 정상 동작의 제약은 검증 항목으로 구별한다.
- API 확장 실험은 같은 머신의 JVM 두 개를 사용한다. 실행을 생성한 뒤 두 번째 JVM을 시작하고 측정 중 Consumer를 정지하며, 완료 검증은 주 JVM에서 수행한다. 운영용 다중 노드 서비스 검색·제어 전파·relay 리더 선출까지 구현한 것은 아니다.
- Redis Stream relay는 한 활성 소유자를 전제로 한다. 같은 consumer 이름으로 자신의 PEL을 재시작 후 읽는다. 여러 relay의 소유권·`XAUTOCLAIM` 경쟁은 별도 확장 과제다.
- Redis Cluster 실험은 실제 master 3개에 대한 명령·슬롯 분포 실험이며 쿠폰 API 전체의 처리량 실험과 구분한다. 같은 쿠폰 재고를 여러 키로 분할하는 구현은 포함하지 않는다.
- Kafka 3.8.1, Redis 7 계열, MySQL 8.0을 사용한다. 실행 시 이미지 digest와 실제 패치 버전을 [인프라 기록](results/infra/events.jsonl)에 남긴다. 기존 2026-09-12 벤치마크의 Kafka 3.7.1과 환경이 다르다.
- 로컬 Docker와 다른 프로세스가 CPU·디스크를 공유한다. 수치는 이 환경의 관측값이며 운영 용량이나 장애 무손실 보증이 아니다. 모든 장애 조합을 전수 검증한 것은 아니다.
- worker 수 제한은 실행별이며 전역 제한이 아니다. 요청 잠금·실행 정보·토픽·Redis 기록의 보존 정책과 장기 장애 중 재시도 대기열의 상한은 구현하지 않았다. 실행 완료 후 `paused=true, workers=0`이 필요하다.

Redis 장애 시 키가 없다고 총량을 자동 초기화하지 않는다. 복구하려면 확정 발급과 미완료 예약을 함께 대조해야 한다. 수동 승격·Sentinel·Cluster의 구체적 명령은 [인프라 안내](infra/README.md)에 정리했다.
