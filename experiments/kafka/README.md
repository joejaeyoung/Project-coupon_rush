# Kafka 파티션·Consumer 확장과 강제 종료 실험

실제 쿠폰 API, Redis 판정, Kafka, MySQL을 사용한다. `run.py`가 요청별 시작 시각과 Consumer의 DB 트랜잭션 완료 후 관측 시각을 결합하고, Kafka 오프셋과 DB·Redis 수량을 대조한다.

## 실행

Python 3.10 이상, Java 17, Docker Compose가 필요하다. Python 외부 패키지는 사용하지 않는다. 저장소 루트에서 전용 인프라를 시작한다.

```bash
docker compose -f experiments/kafka/compose.yml up -d --wait
cd 3-coupon_redis_kafka/spring
./gradlew --no-daemon --max-workers=4 bootJar labBootJar
cd ../..
python3 experiments/kafka/run.py --mode smoke --java /path/to/jdk17/bin/java
python3 experiments/kafka/run.py --mode all --java /path/to/jdk17/bin/java
```

`--java`를 생략하면 PATH의 Java를 사용한다. 실행한 Java 버전과 JAR SHA-256을 결과에 저장한다. 실험 후 인프라를 중지한다.

```bash
docker compose -f experiments/kafka/compose.yml down
```

Compose 프로젝트는 `coupon-kafka-lab`이며 전용 볼륨을 사용한다. 호스트 포트는 MySQL `53306`, Redis `56379`, Kafka `59092`, 실험 앱 `58080`이고 루프백 주소에 바인딩한다. 포트가 사용 중이면 해당 프로세스를 종료하지 말고 실험 설정을 변경한다.

## 1. 파티션·Consumer 비교

| 조건 | 값 |
| --- | --- |
| 구성 | P1/C1, P3/C1, P3/C3 |
| 키 분포 | 쿠폰 1개 집중, 쿠폰 30개 분산 |
| 본 부하 | 요청 6,000건, HTTP 작업자 64개, 요청마다 서로 다른 userId |
| 재고 | 전체 요청이 통과할 수량: 집중 6,000개, 분산 쿠폰당 200개 |
| 워밍업 | 매 실행마다 별도 쿠폰 30개로 1,500건 |
| 반복 | 조합별 3회, 반복마다 실행 순서를 고정 시드로 섞음 |
| 앱 | 매 실행 새 JVM, 힙 512MB, ActiveProcessorCount 4, Hikari 최대 10 |
| Kafka | 단일 Broker, RF 1, 키 couponId, BATCH, max.poll.records 500 |
| 장애 감지 설정 | session.timeout.ms 10,000, heartbeat.interval.ms 3,000 |

매 실행에서 새 토픽·그룹·쿠폰을 만든다. 파티션 수를 늘린 기존 토픽을 재사용하지 않는다. 파티션의 실제 배정과 파티션별 처리 건수를 저장한다. 균등한 쿠폰 수가 균등한 파티션 분포를 뜻하지는 않는다.

**요청→DB p95**는 Python HTTP 작업자의 요청 시작 시각부터 Spring `RecordInterceptor.success` 시각까지다. Consumer의 JPA 트랜잭션이 반환된 후 기록하므로 DB 커밋 완료를 관측한다. 앱과 부하 발생기는 같은 호스트의 epoch 시계를 사용하며, 완료 시각의 해상도는 1ms다. DB 커밋 직후 콜백까지의 오버헤드도 포함한다. HTTP p95는 별도 열이다.

Kafka Lag는 Admin API에서 커밋 오프셋을 먼저, 로그 끝 오프셋을 다음으로 읽어 합산한다. 샘플 수집 응답 후 100ms를 기다리므로 기록한 최대 Lag는 관측된 최대값이다. 두 오프셋 조회는 원자적인 스냅샷이 아니다. `lag_clear_after_last_http_ms`는 마지막 HTTP 응답 이후, DB 완료 건수와 Lag 0을 함께 확인한 첫 샘플까지다.

백분위는 정렬된 원본의 선형 보간(Type 7)으로 계산한다. `db_completion_rate_per_s`는 요청 시작부터 마지막 DB 완료까지의 전체 처리율이며 입력 속도의 영향도 받는다. 원본 요청·완료 시각을 함께 보관해 다시 계산할 수 있다.

이 부하는 Consumer 비교를 위해 모든 요청을 통과시킨다. 기존 1,000 VU·재고 100개 실험의 성공·탈락 혼합 HTTP 수치와 직접 비교하지 않는다. 운영 처리량이나 다중 Broker 내구성 실험으로 해석하지 않는다.

## 2. DB 커밋 후 SIGKILL과 재전달

1. 이벤트 1건을 저장하고 오프셋 1 커밋을 확인한다.
2. Consumer를 일시 정지한 상태에서 요청 50건을 보내, Broker 끝 오프셋 51·Lag 50·DB 행 1개를 확인한다. API의 `202`만으로 Kafka 접수를 판정하지 않는다.
3. Consumer를 재개한다. offset 1 이벤트의 DB 트랜잭션 반환 후, `RecordInterceptor.success`에서 해당 DB 행이 별도 조회로 보이는지와 Broker의 커밋 오프셋이 여전히 1인지 확인한다.
4. 체크포인트를 파일에 fsync하고 Consumer 스레드를 대기시킨다. 외부 실행기가 MySQL·Kafka CLI로 DB 행 2개·오프셋 1을 확인한 다음 Java 프로세스에 **SIGKILL**을 보낸다. 종료 코드 `-9`를 단언한다.
5. 프로세스가 없는 상태에서도 DB 2개·오프셋 1을 다시 확인한다. 같은 토픽·그룹으로 앱을 재시작한다.
6. **같은 partition 0 / offset 1 / userId**의 재전달을 확인한다. 남은 이벤트와 함께 처리한 결과가 DB 행 51개·발급 수량 51·중복 그룹 0·오프셋 51·Lag 0인지 단언한다.

이 체크포인트는 **BATCH 모드 전용**이다. Spring Kafka 3.3.1의 listener 호출 뒤 콜백을 이용한다. RECORD 모드에서는 콜백 전에 오프셋을 커밋할 수 있어 앱 시작 시 BATCH 여부를 검사한다. [Spring Kafka 해당 구현](https://github.com/spring-projects/spring-kafka/blob/v3.3.1/spring-kafka/src/main/java/org/springframework/kafka/listener/KafkaMessageListenerContainer.java).

`kill_to_recovered_ms`에는 외부 상태 조회, 새 JVM 시작과 그룹 재가입 시간이 포함된다. Broker의 장애 감지 시간만을 뜻하지 않는다.

기존 Consumer의 `(couponId, userId)` 존재 조회를 그대로 시험한다. DB 유일 제약을 새로 추가하지 않았으며, 여러 독립 그룹의 동시 중복 처리나 Redis 차감→Kafka 전송 실패까지 검증하는 실험은 아니다.

## 코드와 결과

- 본 구현: `3-coupon_redis_kafka/spring/src/main/`. 토픽·파티션·concurrency 설정과 명시적인 BATCH 커밋.
- 계측·주입: `3-coupon_redis_kafka/spring/src/lab/`. `labBootJar`에만 들어간다. 일반 `bootJar`에는 `/lab` API와 장애 주입 클래스가 포함되지 않는다.
- 실행 결과: `experiments/kafka/out/<UTC 실행 시각>/`. 요청·완료 원본은 gzip JSONL, Lag와 DB·Redis 상태는 JSON, 비교 요약은 CSV.
- 실패하면 `failure.json`과 이미 수집한 증거를 남기고 종료한다. 본 실행은 `completion.json`의 `passed: true`와 각 검증식을 모두 통과해야 완료다.

같은 머신의 MySQL·Redis·Kafka와 JVM·부하 발생기가 자원을 공유한다. 단일 호스트, 닫힌 루프 HTTP 부하, 워밍업과 표본 수의 범위를 결과와 함께 읽어야 한다.
