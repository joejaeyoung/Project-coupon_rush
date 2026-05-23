# Kafka 완전 정복
## 선착순 쿠폰 시스템으로 배우는 Kafka

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
클라이언트 → 서버 → Kafka에 메시지 발행 → 즉시 202 응답
                              ↓
                        Consumer → DB 저장 (나중에, 천천히)
```
- 클라이언트는 즉시 응답받음
- DB 처리는 Consumer가 여유 있게 처리
- 피크 트래픽을 Kafka가 버퍼링 → DB 부하 평준화
- 메시지가 Kafka에 저장되므로 Consumer 장애 시에도 유실 없음

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
- **이 프로젝트**: 단순하게 Partition 1개로 시작

### Offset — 메시지 위치

Offset은 Partition 안에서 메시지의 **순서 번호**다. Consumer가 어디까지 읽었는지 추적한다.

```
Partition 0: [0:msg1] [1:msg2] [2:msg3] [3:msg4] ...
                                          ↑
                              Consumer가 여기까지 읽음 (offset=2 커밋)
```

Kafka는 메시지를 Consumer가 읽어도 **삭제하지 않는다**. Offset만 기억한다. 덕분에:
- Consumer가 장애 후 재시작해도 마지막 Offset부터 다시 읽음 (유실 없음)
- 다른 Consumer Group이 같은 Topic을 처음부터 다시 읽을 수 있음

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

### At-least-once (최소 1번) ← 이 프로젝트
메시지 유실은 없지만 중복 처리될 수 있음. 처리 완료 후 Offset 커밋.

```
Consumer: 처리 중 → 장애 발생 (Offset 미커밋)
재시작: 같은 메시지를 다시 읽음 → 중복 처리 가능
```

**멱등성(Idempotency)** 으로 해결: 같은 요청을 여러 번 처리해도 결과가 같게 설계.
이 프로젝트에서는 Redis의 `SISMEMBER`로 이미 발급된 userId를 걸러냄 → 중복 발급 방지.

### Exactly-once (정확히 1번)
메시지가 정확히 한 번 처리됨. Kafka Transactions 설정 필요.
- 가장 안전하지만 구현이 복잡하고 성능 오버헤드가 있음
- 금융 거래처럼 절대 중복이 허용 안 될 때 사용

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

Spring Kafka에서는 `@RetryableTopic` 어노테이션으로 쉽게 설정할 수 있다:

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

---

## 7. Consumer Lag — 처리가 밀리고 있나?

**Consumer Lag**는 Producer가 보낸 메시지 수와 Consumer가 처리한 메시지 수의 차이다.

```
Producer: Offset 1000까지 발행
Consumer: Offset 850까지 처리
Consumer Lag: 150 (150개 밀림)
```

Lag가 계속 증가한다면 Consumer의 처리 속도가 Producer의 발행 속도를 못 따라가는 것 → Consumer 인스턴스나 Partition을 늘려야 한다.

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
  1. Redis로 수량/중복 검증 (빠름)
  2. kafka.send("coupon-issue-request", message)
  3. 즉시 202 Accepted 응답 ← 클라이언트가 여기서 반환받음
         ↓ (비동기)
  CouponIssueConsumer.consume(message)
  4. DB에 IssuedCoupon 저장
  5. 실패 시 DLQ로 이동
```

**메시지 설계:**
```java
public record CouponIssueMessage(
    Long couponId,
    Long userId,
    LocalDateTime requestedAt
) {}
```

**피크 트래픽 흡수 원리:**
```
10,000 RPS 요청 →  Kafka (버퍼) → Consumer 500 RPS로 천천히 처리
     ↑                                    ↑
  즉시 202 응답              DB는 감당 가능한 속도로만 받음
```

---

## 9. Kafka vs Redis Pub/Sub

둘 다 메시지 발행/구독을 지원하지만 목적이 다르다.

| 항목 | Kafka | Redis Pub/Sub |
|------|-------|---------------|
| 메시지 보관 | 설정한 기간 동안 유지 | 구독자가 없으면 유실 |
| Consumer 오프라인 시 | 재연결 후 밀린 메시지 처리 | 메시지 유실 |
| 처리량 | 초당 수백만 메시지 | 빠르지만 Kafka보다 낮음 |
| 용도 | 이벤트 스트리밍, 비동기 처리 | 실시간 알림, 간단한 pub/sub |

이 프로젝트에서 Kafka를 쓰는 이유: Consumer가 잠시 다운돼도 재시작 후 밀린 발급 요청을 모두 처리할 수 있어야 하기 때문.

---

## 10. 자주 쓰는 CLI 명령어

```bash
# Kafka 컨테이너 접속
docker exec -it kafka bash

# 토픽 목록
/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

# 토픽 생성
/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic coupon-issue-request \
  --partitions 1 --replication-factor 1

# 토픽 상세 정보 (Partition, Replica 등)
/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --describe --topic coupon-issue-request

# 메시지 실시간 소비 (Consumer 역할로 확인)
/opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic coupon-issue-request \
  --from-beginning

# Consumer Lag 확인
/opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe --group coupon-group
```
