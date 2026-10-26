# Kafka 완전 정복
## 선착순 쿠폰 시스템으로 배우는 Kafka

> 2026-10-22 보완: 현재 구현과 학습용 예제를 구분하고 전달·순서 보장의 조건을 정정했다. Redis/Kafka 처리 순서, hot partition, 장애 복구와 선착순 기준은 [멘토링 논의안](mentoring-discussion.md)에 정리한다.

> 실제 재현: Kafka 우선 접수·DB Outbox·Redis Stream과 3브로커 환경은 [실험 모듈](../5-coupon_experiments/README.md), 파티션 분포·증설 시 키 이동·재시도와 리밸런싱의 순서·브로커 장애 결과는 [실증 보고서](experiments-2026-10-22.md)를 참고한다. 이 문서의 기존 Phase 3 단일 브로커 설명과 구분한다. 현재 4단계 [4-coupon_redisHA_kafka](../4-coupon_redisHA_kafka/README.md)는 Lua·Stream 없이 Phase 3에 Kafka ACK 확인과 보상, DB 유니크 제약을 더한 최소 구성이다.

---

## 1. Kafka가 뭔가요?

**Apache Kafka**는 대용량 데이터를 실시간으로 처리하기 위한 **분산 메시지 스트리밍 플랫폼**이다.

쉽게 말하면 **초고속 우체국**이다.
- **Producer(발신자)**: 편지(메시지)를 우체통(Topic)에 넣음
- **Kafka Broker**: 우체국. 편지를 보관하고 배달
- **Consumer(수신자)**: 편지를 꺼내 읽음

### 왜 직접 API를 호출하지 않고 Kafka를 쓰나?

**직접 호출 (동기):**
```
클라이언트 → 서버 → DB 저장 → 응답 (서버가 처리 끝날 때까지 기다림)
```
- 피크 트래픽에 서버/DB 부하가 그대로 몰림
- DB가 느려지면 클라이언트도 느려짐
- 서버 장애 시 요청 유실

**Kafka 사용 (비동기):**
```
클라이언트 → 서버 → Kafka에 메시지 발행 → 접수 확인 후 202 응답
                              ↓
                        Consumer → DB 저장 (나중에, 천천히)
```
- 클라이언트는 DB 저장을 기다리지 않고 접수 응답을 받음
- DB 처리는 Consumer가 여유 있게 처리
- 피크 트래픽을 Kafka가 버퍼링 → DB 부하 평준화
- 브로커가 승인한 메시지는 보존기간과 복제 조건 안에서 Consumer 재시작 후 다시 읽을 수 있음

위 그림은 일반적인 비동기 접수 모델이다. 현재 Phase 3는 먼저 Redis에서 예약하고, `send()`가 반환한 Future를 확인하지 않은 채 `202`를 반환한다. 따라서 현재 응답은 브로커 저장 완료나 최종 발급 완료를 보장하지 않는다.

---

## 2. 핵심 개념

### Topic — 메시지 분류

Topic은 메시지를 분류하는 **채널 이름**이다. 파일 시스템의 폴더와 비슷하다.

```
coupon-issue-request   ← 쿠폰 발급 요청 메시지들
coupon-issue-dlq       ← 처리 실패한 메시지들 (Dead Letter Queue)
order-created          ← 주문 생성 이벤트
```

### Partition — 병렬 처리 단위

Topic은 여러 개의 **Partition**으로 나뉜다. 각 Partition은 독립적인 로그 파일이다.

```
coupon-issue-request (Topic)
├── Partition 0: [msg1, msg2, msg5, msg8, ...]
├── Partition 1: [msg3, msg6, msg9, ...]
└── Partition 2: [msg4, msg7, msg10, ...]
```

- **순서 보장**: Partition **안에서만** 순서가 보장됨. Topic 전체 순서는 보장 안 됨
- **병렬 처리**: Partition이 많을수록 Consumer가 동시에 더 많이 처리 가능
- **이 프로젝트**: 브로커의 토픽 자동 생성이 켜져 있고 파티션 수를 명시하는 애플리케이션 설정은 없다. 아래 CLI 예제는 1개이며 실제 토픽은 `--describe`로 확인한다.

현재 메시지 키는 `couponId`다. 파티션 수와 라우팅 방식이 유지되면 같은 쿠폰 이벤트는 같은 파티션으로 가지만 여러 쿠폰의 요청량까지 균등해지는 것은 아니다. 한 쿠폰에 몰리면 hot partition이 남고, 다른 쿠폰도 같은 파티션을 공유할 수 있다. 파티션 증설은 기존 레코드를 옮기지 않으며 이후 키 배치가 달라질 수 있으므로 행사 도중 변경하는 정책도 논의해야 한다. [Kafka 토픽 변경](https://kafka.apache.org/37/operations/basic-kafka-operations/#modifying-topics)

### Offset — 메시지 위치

Offset은 Partition 안에서 메시지의 **순서 번호**다. Consumer가 어디까지 읽었는지 추적한다.

```
Partition 0: [0:msg1] [1:msg2] [2:msg3] [3:msg4] ...
                                          ↑
                              offset 2까지 처리 완료 → 다음 위치 3을 커밋
```

Kafka는 메시지를 Consumer가 읽었다는 이유로 바로 삭제하지 않는다. 커밋한 offset은 다음에 읽을 위치다. 보존 중인 레코드는 재시작·다른 Consumer Group에서 다시 읽을 수 있지만, 시간·용량 기반 retention이나 compaction 정책에 따라 삭제될 수 있다. 소비 지연이 보존 범위를 넘으면 과거 요청을 복구하지 못할 수 있다. [Kafka 보존 설정](https://kafka.apache.org/37/configuration/topic-level-configs/)

### Broker — Kafka 서버

Broker는 메시지를 저장하고 전달하는 **Kafka 서버 인스턴스**다. 이 프로젝트에서는 1개의 Broker만 쓴다. 프로덕션에서는 보통 3개 이상으로 클러스터를 구성한다.

### Consumer Group — 협력해서 처리

같은 Consumer Group에 속한 Consumer들은 **Topic의 Partition을 나눠서** 처리한다.

```
Topic: coupon-issue-request (Partition 3개)
Consumer Group: coupon-group

Consumer A → Partition 0 담당
Consumer B → Partition 1 담당
Consumer C → Partition 2 담당
```

- Consumer Group 내에서 **Partition 1개는 Consumer 1개만** 담당
- Consumer가 Partition보다 많아도 소용없음 (나머지는 놀게 됨)
- Consumer가 Partition보다 적으면 한 Consumer가 여러 Partition 담당

**리밸런싱(Rebalancing)**: Consumer가 추가되거나 죽으면 Partition 담당이 자동으로 재배치된다.

---

## 3. 메시지 전달 보장 수준

Kafka는 세 가지 전달 보장 수준을 제공한다.

### At-most-once (최대 1번)
메시지가 유실될 수 있지만 중복은 없음. Consumer가 메시지를 읽자마자 Offset을 커밋하고 처리.

```
Consumer: Offset 커밋 → 처리 중 → 장애 발생
결과: 메시지 처리 안 됨 (유실)
```

로그 수집처럼 일부 유실을 허용할 수 있을 때 사용.

### At-least-once (최소 1번) — 현재 Consumer의 설계 의도
처리 완료 후 offset을 커밋해 실패한 처리를 다시 시도하는 방식이다. 재전달에 따른 중복을 고려해야 한다. Producer 접수 실패, 보존기간 초과, 재시도 소진까지 자동으로 해결하는 보장은 아니다.

```
Consumer: 처리 중 → 장애 발생 (Offset 미커밋)
재시작: 같은 메시지를 다시 읽음 → 중복 처리 가능
```

**멱등성(Idempotency)** 으로 해결: 같은 요청을 여러 번 처리해도 결과가 같게 설계.
현재 Consumer는 DB의 `existsByCoupon_IdAndUserId()`로 이미 발급된 사용자를 확인한다. Redis의 `SADD`는 API 앞단의 중복 예약 판정이다. Consumer 조회 후 INSERT 사이 경쟁까지 막는 DB 복합 유일 제약은 엔티티에 선언되어 있지 않아, 모든 병렬 처리·재처리 상황의 멱등성이 완성된 상태는 아니다.

`enable-auto-commit: false`는 Kafka 클라이언트의 주기적 자동 커밋을 끈다. Spring 컨테이너의 실제 커밋 시점은 `AckMode`에 따르며, 이 프로젝트는 이를 명시하지 않았다. 이 설정 하나로 매 요청 즉시 커밋이나 무한 재시도를 약속할 수 없다. [Spring Kafka 컨테이너 문서](https://docs.spring.io/spring-kafka/reference/3.3/kafka/receiving-messages/message-listener-container.html)

### Exactly-once (정확히 1번)
Kafka 트랜잭션은 Kafka에서 읽고 처리해 다른 Kafka 토픽에 쓰는 결과와 소비 offset을 묶을 수 있다. 외부 MySQL 저장까지 같은 원자적 커밋이 되는 것은 아니다. 이 프로젝트의 DB `@Transactional`은 발급 행과 수량 변경을 묶지만 Kafka offset은 별도다. DB 유일 제약과 요청/이벤트 식별자 등을 통해 재처리해도 최종 결과가 한 번만 반영되게 설계해야 한다. [Kafka 전달 의미론](https://kafka.apache.org/37/design/design/#message-delivery-semantics)

---

## 4. KRaft 모드 — Zookeeper 없는 Kafka

### 기존 방식 (Zookeeper 필요)
```
[Kafka Broker] ↔ [Zookeeper]
                      ↑
              브로커 목록, 토픽 정보,
              리더 선출 등 메타데이터 관리
```
Kafka가 Zookeeper에 의존해서 운영할 서비스가 하나 더 필요했다. 개발/테스트 환경에서 불편하고, 두 시스템을 모두 관리해야 해서 복잡하다.

### KRaft 모드 (Kafka 2.8+)
```
[Kafka Broker + Controller]
        ↑
    Raft 합의 알고리즘으로
    메타데이터 자체 관리
```
Kafka가 Raft 합의 알고리즘을 사용해 자체적으로 메타데이터를 관리한다. Zookeeper가 필요 없다.

- **Raft 알고리즘**: 분산 시스템에서 여러 노드가 하나의 값에 합의하는 알고리즘. 리더를 선출하고 데이터 복제를 관리
- **이 프로젝트**: KRaft 모드로 Kafka 1개 노드만 실행 (개발 환경)

```yaml
# docker-compose.yml 설정 의미
KAFKA_PROCESS_ROLES: "broker,controller"  # 하나의 노드가 두 역할 모두
KAFKA_NODE_ID: "1"
KAFKA_CONTROLLER_QUORUM_VOTERS: "1@kafka:9093"  # 리더 투표권을 가진 노드
```

---

## 5. Kafka의 두 가지 리스너 (이 프로젝트)

```
[호스트 (IntelliJ, k6)]  →  localhost:9092  →  EXTERNAL 리스너
[Docker 컨테이너 (Spring)] →  kafka:9094    →  INTERNAL 리스너
```

```yaml
KAFKA_LISTENERS: "EXTERNAL://0.0.0.0:9092,INTERNAL://0.0.0.0:9094,CONTROLLER://0.0.0.0:9093"
KAFKA_ADVERTISED_LISTENERS: "EXTERNAL://localhost:9092,INTERNAL://kafka:9094"
```

- **ADVERTISED_LISTENERS**: Kafka가 클라이언트에게 "나한테 이 주소로 접속해"라고 알려주는 주소
- 호스트에서 연결 → Kafka가 `localhost:9092` 알려줌 → 호스트에서 잘 접속됨
- Docker 컨테이너에서 연결 → Kafka가 `kafka:9094` 알려줌 → 컨테이너 내부에서 잘 접속됨

---

## 6. DLQ (Dead Letter Queue)

이 절의 retry/DLT 코드는 학습용 대안이며 현재 구현에는 없다. 실제 재시도 횟수·대상 예외·실패 후 처리는 적용된 Spring Kafka 오류 처리기에 따라 확인해야 한다.

Consumer가 메시지를 처리하다 계속 실패하면 어떻게 될까?

```
메시지 처리 실패 → 재시도 → 재시도 → 재시도 → ...
이 메시지가 막혀서 뒤의 메시지들도 처리 안 됨 ❌
```

**DLQ**는 일정 횟수 이상 실패한 메시지를 별도 Topic으로 이동시켜 나머지가 계속 처리되게 한다.

```
coupon-issue-request (정상 Topic)
       ↓ 3번 실패
coupon-issue-request-dlt (Dead Letter Topic)
       ↓
운영자가 모니터링하고 수동으로 재처리하거나 원인 분석
```

Spring Kafka의 `@RetryableTopic`으로 retry/DLT 토픽을 구성하는 예제:

```java
@RetryableTopic(
    attempts = "3",                    // 최대 3번 시도
    backoff = @Backoff(delay = 1000),  // 재시도 간격 1초
    dltTopicSuffix = "-dlt"            // 실패 시 coupon-issue-request-dlt로 이동
)
@KafkaListener(topics = "coupon-issue-request")
public void consume(CouponIssueMessage message) {
    // 처리 로직
}
```

실패 요청을 별도 retry 토픽으로 보내는 동안 뒤 요청이 먼저 처리될 수 있어 원래 토픽의 순서를 그대로 유지하지 못한다. 선착순을 Kafka 기록 순서로 정한다면 해당 파티션을 멈춰 재시도할지, 실패 요청을 제외하고 진행할지부터 합의해야 한다. [Spring Kafka 비차단 재시도](https://docs.spring.io/spring-kafka/reference/retrytopic/how-the-pattern-works.html)

---

## 7. Consumer Lag — 처리가 밀리고 있나?

**Consumer Lag**는 Producer가 보낸 메시지 수와 Consumer가 처리한 메시지 수의 차이다.

```
Producer: Offset 1000까지 발행
Consumer: Offset 850까지 처리
Consumer Lag: 150 (150개 밀림)
```

Lag가 계속 증가하면 파티션별 요청량·Consumer 처리시간·DB 지연을 함께 본다. 분산된 쿠폰 트래픽은 파티션과 Consumer를 늘려 처리할 여지가 있지만, 같은 쿠폰 키의 hot partition은 Consumer 증설만으로 나뉘지 않는다. 같은 그룹에서 파티션 수보다 많은 Consumer는 남는 파티션을 배정받지 못한다.

```bash
# Consumer Lag 확인
docker exec -it kafka bash
/opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe --group coupon-group
```

---

## 8. 이 프로젝트에서 Kafka 활용 (Phase 3)

```
POST /api/coupons/{couponId}/issue
         ↓
  1. Redis 기간 검증 → SADD → DECR (별도 명령)
  2. kafkaTemplate.send("coupon-issue", couponId.toString(), event)
  3. send 결과 Future 확인 없이 202 Accepted 응답
         ↓ (비동기)
  CouponKafkaConsumer.consume(event)
  4. DB 중복 조회 → 수량 증가 + IssuedCoupon 저장 (DB 트랜잭션)
  5. offset 커밋/실패 처리는 컨테이너 정책 적용 (DLQ 미구현)
```

**기존 Phase 3의 메시지 필드 (`CouponIssueEvent`):**
```java
private Long couponId;
private Long userId;
private String issuedAt;
```

기존 Phase 3 이벤트에는 `requestId`나 발급 순번이 없다. 이 단계에는 발급 완료 목록 조회 API만 있고 요청 상태 조회·SSE/WebSocket 알림은 없다. 후속 실험 모듈은 Kafka 메시지에 `requestId`를 담고, DB 요청 원장에 예약·완료 순번과 partition/offset을 연결하며 요청 상태 API를 제공한다. SSE/WebSocket 알림은 후속 모듈에도 구현하지 않았다. Redis 예약 순서와 Kafka에 기록되는 순서도 다를 수 있으므로 "선착순"을 어느 시점으로 정의할지는 [멘토링 논의안](mentoring-discussion.md)에서 별도로 다룬다.

**피크 트래픽 흡수 원리 (가정 수치):**
```
10,000 RPS 요청 →  Kafka (버퍼) → Consumer 500 RPS로 천천히 처리
     ↑                                    ↑
  즉시 202 응답              DB는 감당 가능한 속도로만 받음
```

현재처럼 Redis가 앞에 있으면 탈락 요청은 Kafka에 들어가지 않는다. Kafka를 앞으로 옮기면 전체 접수 요청을 저장하는 대신 재고 소진·중복 결과도 비동기적으로 전달해야 한다. 어느 구조든 유입이 처리량을 계속 초과하면 backlog가 쌓이므로 대기시간·보존용량·접수 제한을 함께 설계한다.

---

## 9. Kafka vs Redis Pub/Sub

둘 다 메시지 발행/구독을 지원하지만 목적이 다르다.

| 항목 | Kafka | Redis Pub/Sub |
|------|-------|---------------|
| 메시지 보관 | 설정한 기간 동안 유지 | 구독자가 없으면 유실 |
| Consumer 오프라인 시 | 보존 범위 안에서 재연결 후 처리 | 오프라인 동안 수신하지 못함 |
| 처리량 | 메시지 크기·파티션·복제·디스크 등에 따라 측정 | 메시지 크기·구독자 수·네트워크 등에 따라 측정 |
| 용도 | 이벤트 스트리밍, 비동기 처리 | 실시간 알림, 간단한 pub/sub |

이 프로젝트에서 Kafka를 쓰는 이유는 DB 쓰기를 분리하고 Consumer 재시작 후 대기 이벤트를 다시 처리하기 위해서다. 이를 운영 보장으로 만들려면 Producer 확인, 복제·retention, 재처리·멱등성을 함께 갖춰야 한다.

---

## 10. 자주 쓰는 CLI 명령어

```bash
# Kafka 컨테이너 접속
docker exec -it kafka bash

# 토픽 목록
/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

# 토픽 생성
/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic coupon-issue \
  --partitions 1 --replication-factor 1

# 토픽 상세 정보 (Partition, Replica 등)
/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --describe --topic coupon-issue

# 메시지 실시간 소비 (Consumer 역할로 확인)
/opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic coupon-issue \
  --from-beginning

# Consumer Lag 확인
/opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe --group coupon-group
```
