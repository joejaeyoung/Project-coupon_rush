# Phase 0. Docker Compose & JPA 기초
## 선착순 쿠폰 시스템 구축기 (1)

> 이 글은 선착순 쿠폰 발급 시스템을 단계적으로 구현하는 시리즈의 첫 번째 글입니다.
> 본격적인 구현 전에 인프라 환경을 세팅하고, JPA와 Spring Boot의 기초 개념을 정리합니다.

---

## 이번 글에서 다루는 것

- Docker와 Docker Compose의 핵심 개념
- `docker-compose up -d` 한 번으로 MySQL + Redis + Kafka 실행하기
- JPA 영속성 컨텍스트가 실제로 하는 일
- Spring Boot의 계층 구조 (Controller → Service → Repository)

---

## 1. Docker / Docker Compose

### Image vs Container

Docker를 처음 배울 때 가장 혼란스러운 부분이 Image와 Container의 차이다.

| 개념 | 비유 | 설명 |
|------|------|------|
| Image | 붕어빵 틀 | 실행 환경과 코드가 담긴 **설계도**. 변경 불가 |
| Container | 붕어빵 | Image를 실행한 **인스턴스**. 같은 Image로 여러 개 생성 가능 |

```bash
# Image 다운로드
docker pull mysql:8.0

# Image로 Container 실행
docker run -d --name my-mysql mysql:8.0

# 같은 Image로 또 다른 Container 생성 가능
docker run -d --name my-mysql-2 mysql:8.0
```

Image는 레이어(Layer) 구조로 이루어져 있다. `mysql:8.0`은 `ubuntu` 위에 MySQL 바이너리를 얹은 형태다. 이미 다운된 레이어는 재사용하기 때문에 디스크 효율이 높다.

### Volume — 컨테이너가 삭제돼도 데이터가 남는 이유

Container는 기본적으로 **휘발성**이다. `docker rm`으로 삭제하면 그 안의 데이터도 함께 사라진다.

```bash
docker run -d mysql:8.0
docker rm <container_id>
# DB 데이터 전부 날아감 ❌
```

Volume은 Container 밖의 호스트 디스크에 데이터를 저장한다.

```yaml
volumes:
  - mysql_data:/var/lib/mysql   # Named Volume (Docker가 관리)
  - ./mysql/conf.d:/etc/mysql/conf.d  # Bind Mount (로컬 디렉토리 직접 연결)
```

- **Named Volume** (`mysql_data:/var/lib/mysql`): Docker가 `/var/lib/docker/volumes/` 어딘가에 관리. `docker volume ls`로 확인 가능.
- **Bind Mount** (`./mysql/conf.d:/etc/mysql/conf.d`): 로컬 디렉토리를 Container 내부 경로에 마운트. 설정 파일 주입에 주로 사용.

### Network — 서비스끼리 이름으로 통신하는 이유

같은 `docker-compose.yml` 안의 서비스들은 자동으로 같은 네트워크에 속한다. 덕분에 IP 대신 **서비스 이름**으로 통신할 수 있다.

```yaml
# Spring 서비스에서 MySQL 연결 시
SPRING_DATASOURCE_URL: jdbc:mysql://mysql:3306/coupon_db
#                                   ^^^^^ 서비스 이름으로 접근
```

브라우저(호스트)에서는 `localhost:3306`으로 접근하고, 컨테이너 내부에서는 `mysql:3306`으로 접근한다.

### healthcheck + depends_on

```yaml
mysql:
  healthcheck:
    test: ["CMD", "mysqladmin", "ping", "-h", "localhost"]
    interval: 10s
    retries: 5

spring:
  depends_on:
    mysql:
      condition: service_healthy  # mysql이 healthy 상태가 된 후에 spring 시작
```

단순 `depends_on`만 쓰면 MySQL 프로세스가 시작됐어도 아직 연결을 받을 준비가 안 된 상태에서 Spring이 연결을 시도해 실패할 수 있다. `condition: service_healthy`로 실제로 준비된 이후에 시작하도록 보장한다.

### Kafka KRaft 모드

기존 Kafka는 메타데이터(토픽 목록, 브로커 상태 등)를 Zookeeper라는 별도 서비스에 저장했다. 운영할 서비스가 하나 더 늘어나는 셈이다.

**KRaft 모드**는 Kafka 자체가 Raft 합의 알고리즘으로 메타데이터를 관리한다. Zookeeper 없이 Kafka만으로 클러스터를 구성할 수 있어서 개발/테스트 환경에서 훨씬 간단해진다.

```yaml
kafka:
  environment:
    KAFKA_PROCESS_ROLES: broker,controller   # 하나의 노드가 두 역할 담당
    KAFKA_NODE_ID: 1
    KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
```

`KAFKA_PROCESS_ROLES: broker,controller`는 개발 환경에서만 쓴다. 프로덕션에서는 broker와 controller 역할을 분리한 노드들로 클러스터를 구성한다.

### Dockerfile — 내 앱을 이미지로 만드는 설계도

MySQL, Redis는 DockerHub에서 완성된 이미지를 그냥 받아 쓴다. 하지만 Spring Boot 앱은 내가 직접 만든 코드니까 **이미지를 직접 빌드**해야 한다. 그 방법을 적어둔 파일이 Dockerfile이다.

```dockerfile
# Stage 1 — 빌드
FROM gradle:8.5-jdk17 AS build
WORKDIR /home/gradle/src

COPY build.gradle settings.gradle gradlew ./
COPY gradle ./gradle
RUN chmod +x ./gradlew
RUN ./gradlew dependencies          # 의존성만 먼저 다운 (캐시 활용)

COPY src ./src
RUN ./gradlew build --no-daemon -x test  # .jar 생성

# Stage 2 — 실행
FROM openjdk:17-jdk-slim
WORKDIR /app
COPY --from=build /home/gradle/src/build/libs/coupon-0.0.1-SNAPSHOT.jar app.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
```

#### 멀티 스테이지 빌드

두 개의 `FROM`이 있다는 게 핵심이다.

- **Stage 1 (build)**: Gradle이 설치된 무거운 이미지 안에서 소스코드를 컴파일해 `.jar`를 만든다
- **Stage 2 (run)**: `.jar` 파일만 가져와서 가벼운 JDK 이미지로 실행한다

최종 이미지에는 Gradle, 소스코드, 빌드 캐시가 포함되지 않는다. 이미지 크기가 훨씬 작아진다.

#### 의존성을 먼저 복사하는 이유

```dockerfile
COPY build.gradle settings.gradle gradlew ./
RUN ./gradlew dependencies   # 의존성 레이어 캐시

COPY src ./src               # 소스코드는 나중에
RUN ./gradlew build
```

Docker는 각 명령어를 **레이어(Layer)** 단위로 캐싱한다. `build.gradle`이 바뀌지 않으면 `./gradlew dependencies` 레이어는 캐시된 것을 재사용한다. 소스코드만 수정했을 때 의존성을 다시 다운받지 않아 빌드가 빨라진다.

순서를 반대로 하면(`COPY src` 먼저) 소스코드가 한 글자만 바뀌어도 의존성 레이어가 무효화돼 매번 수백 MB를 다시 다운받게 된다.

#### docker-compose.yml과의 관계

```yaml
spring:
  build:
    context: ./coupon      # 이 디렉토리를 기준으로
    dockerfile: Dockerfile  # 이 파일을 읽어 이미지 빌드
```

`docker-compose up`을 실행하면 Docker가 Dockerfile을 읽어 Spring 이미지를 빌드한 뒤 컨테이너로 실행한다. MySQL/Redis처럼 `image: mysql:8.0` 한 줄로 끝나는 게 아니라, **소스코드로 이미지를 직접 만드는** 것이 차이점이다.

> **개발 중 팁**: 코드를 매번 빌드해서 Docker로 실행하면 느리다. 실제로는 MySQL/Redis/Kafka만 `docker-compose up`으로 띄워두고, Spring Boot는 IntelliJ에서 직접 실행하는 방식을 주로 쓴다. Docker 배포는 최종 검증 단계에서 사용한다.

---

## 2. JPA 영속성 컨텍스트

### JPA가 없었다면?

```java
// JPA 없이 JDBC로 직접 작성했을 코드
Connection conn = dataSource.getConnection();
PreparedStatement ps = conn.prepareStatement(
    "INSERT INTO coupon (name, status, total_quantity) VALUES (?, ?, ?)"
);
ps.setString(1, coupon.getName());
ps.setString(2, coupon.getStatus().name());
ps.setInt(3, coupon.getTotalQuantity());
ps.executeUpdate();
```

JPA를 쓰면 이 코드가 `couponRepository.save(coupon)` 한 줄이 된다. 단순히 SQL을 줄여주는 것 이상으로, **객체와 DB 행(Row)을 매핑**해서 객체 중심으로 개발할 수 있게 해준다.

### 영속성 컨텍스트 (Persistence Context)

영속성 컨텍스트는 JPA가 관리하는 **엔티티 저장소**다. DB가 아니라 애플리케이션 메모리 안에 있다.

```
[Entity] ←→ [영속성 컨텍스트] ←→ [DB]
```

엔티티의 생명주기:

```
비영속 (new)    →   영속 (managed)   →   준영속 (detached)   →   삭제 (removed)
Coupon c = new Coupon()   em.persist(c)     em.detach(c)         em.remove(c)
```

Spring Data JPA에서는 `EntityManager`를 직접 쓸 일이 거의 없다. `Repository.save()`가 내부적으로 `em.persist()`를 호출한다.

### 1차 캐시

같은 트랜잭션 안에서 같은 `id`로 두 번 조회하면 두 번째는 DB 조회 없이 영속성 컨텍스트에서 바로 반환한다.

```java
@Transactional
public void example() {
    Coupon c1 = couponRepository.findById(1L).get(); // DB 조회 ✅
    Coupon c2 = couponRepository.findById(1L).get(); // 캐시에서 반환 ✅ (SQL 없음)
    System.out.println(c1 == c2); // true — 같은 객체
}
```

이 덕분에 같은 트랜잭션 안에서는 동일한 엔티티 인스턴스의 동일성이 보장된다.

### 변경 감지 (Dirty Checking)

영속성 컨텍스트는 엔티티를 처음 조회할 때 **스냅샷**을 저장해둔다. 트랜잭션 커밋 시점에 현재 상태와 스냅샷을 비교해서, 바뀐 필드가 있으면 **자동으로 UPDATE 쿼리**를 생성한다.

```java
@Transactional
public void issueCoupon(Long couponId) {
    Coupon coupon = couponRepository.findById(couponId).get();
    coupon.setIssuedQuantity(coupon.getIssuedQuantity() + 1); // 수정
    // couponRepository.save(coupon) 호출 불필요!
    // 트랜잭션 종료 시 자동으로 UPDATE 쿼리 실행
}
```

명시적으로 `save()`를 호출하지 않아도 된다. 다만, `@Transactional`이 있는 메서드 안에서만 동작한다. 트랜잭션 밖에서는 변경해도 DB에 반영되지 않는다.

### @Transactional의 동작 원리

`@Transactional`은 **AOP 프록시** 기반으로 동작한다. Spring이 실제 Service 클래스를 감싸는 프록시 객체를 만들고, 메서드 호출 전후로 트랜잭션 시작/커밋(롤백)을 처리한다.

```
[Controller] → [CouponService 프록시] → [CouponService 실제 객체]
                     ↑
              트랜잭션 시작 / 커밋 / 롤백
```

중요한 제약:
- **같은 클래스 내부 호출**에서는 프록시를 거치지 않아 `@Transactional`이 무시된다
- `private` 메서드에는 적용 안 됨

```java
@Service
public class CouponService {
    @Transactional
    public void outer() {
        inner(); // 프록시를 거치지 않아 inner()의 @Transactional 무시됨 ⚠️
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void inner() { ... }
}
```

### 주요 어노테이션 정리

```java
@Entity           // 이 클래스가 DB 테이블과 매핑됨
@Table(name = "coupon")  // 테이블 이름 명시 (생략하면 클래스명 그대로)
@Id              // 기본 키(PK)
@GeneratedValue(strategy = GenerationType.IDENTITY)  // auto_increment
@Column(nullable = false, length = 100)  // 컬럼 제약 조건
@Enumerated(EnumType.STRING)  // Enum을 문자열로 저장 (ORDINAL은 위험)
@ManyToOne       // N:1 관계
@JoinColumn(name = "coupon_id")  // FK 컬럼명 명시
```

`@Enumerated(EnumType.ORDINAL)`은 Enum의 순서(0, 1, 2...)로 저장한다. Enum 순서가 바뀌거나 중간에 항목이 추가되면 기존 데이터 의미가 달라지는 치명적인 버그가 생긴다. **항상 `EnumType.STRING`을 쓸 것.**

---

## 3. Spring Boot 계층 구조

### Controller → Service → Repository

```
HTTP 요청
    ↓
Controller  — 요청을 받아 Service를 호출. 비즈니스 로직 없음
    ↓
Service     — 실제 비즈니스 로직 처리. @Transactional 여기에
    ↓
Repository  — DB 접근. SQL(JPQL) 작성
    ↓
DB
```

각 계층이 하는 일이 명확히 나뉘어야 한다. Controller에서 DB를 직접 조회하거나, Repository에서 HTTP 응답 객체를 다루면 안 된다.

```java
// ❌ 나쁜 예 — Controller에 비즈니스 로직이 섞임
@PostMapping("/{couponId}/issue")
public ResponseEntity<?> issue(@PathVariable Long couponId, Long userId) {
    Coupon coupon = couponRepository.findById(couponId).orElseThrow();
    if (coupon.getIssuedQuantity() >= coupon.getTotalQuantity()) {
        throw new RuntimeException("재고 소진");
    }
    coupon.setIssuedQuantity(coupon.getIssuedQuantity() + 1);
    couponRepository.save(coupon);
    return ResponseEntity.ok(...);
}

// ✅ 좋은 예 — Controller는 위임만
@PostMapping("/{couponId}/issue")
public ResponseEntity<?> issue(@PathVariable Long couponId, Long userId) {
    return couponService.issueCoupon(couponId, userId);
}
```

### 공통 응답 포맷 — ApiResponse

API 응답을 통일하면 프론트엔드나 클라이언트가 일관되게 처리할 수 있다.

```json
// 성공
{ "success": true, "data": { ... }, "message": null }

// 실패
{ "success": false, "data": null, "message": "이미 발급받은 쿠폰입니다." }
```

```java
@Getter
@AllArgsConstructor
public class ApiResponse<T> {
    private boolean success;
    private T data;
    private String message;

    public static <T> ApiResponse<T> success(T data) {
        return new ApiResponse<>(true, data, null);
    }

    public static <T> ApiResponse<T> error(String message) {
        return new ApiResponse<>(false, null, message);
    }
}
```

---

## Phase 0 구현 결과

### 환경 구성 체크리스트

```bash
# 1. 인프라 실행
docker-compose up -d

# 2. 상태 확인
docker-compose ps

# 3. MySQL 접속 확인
docker exec -it mysql mysql -u myuser -pmypassword coupon_db

# 4. Redis 확인
docker exec -it redis redis-cli ping
# → PONG

# 5. Kafka 토픽 확인 (Phase 3에서 실제로 사용)
docker exec -it kafka kafka-topics.sh --list --bootstrap-server localhost:9092
```

### 도메인 모델

이 프로젝트에서 사용하는 엔티티는 3개다:

```
Coupon (쿠폰 정보)
  ├── id, name, description
  ├── couponType: FIXED_AMOUNT / PERCENTAGE
  ├── status: ACTIVE / INACTIVE / EXHAUSTED / EXPIRED
  ├── totalQuantity (총 발급 가능 수량)
  └── issuedQuantity (현재 발급된 수량)

IssuedCoupon (발급된 쿠폰)
  ├── coupon → Coupon (FK: coupon_id)
  ├── userId
  ├── status: ISSUED / USED / EXPIRED / CANCELLED
  └── issuedAt, expiredAt, usedAt

CouponUsageHistory (사용 이력)
  ├── issuedCoupon → IssuedCoupon (FK: issued_coupon_id)
  ├── userId, orderId
  └── discountAmount, usedAt
```

---

## 다음 글 예고

Phase 1에서는 실제로 발급 API를 구현하면서 **동시성 문제**를 직접 경험한다.
1000명이 동시에 요청을 보내면 정확히 100개만 발급되어야 하는데, 아무것도 하지 않으면 어떤 일이 벌어질까?

> 다음: Phase 1. DB Lock으로 동시성 제어 — 선착순 쿠폰 시스템 구축기 (2)
