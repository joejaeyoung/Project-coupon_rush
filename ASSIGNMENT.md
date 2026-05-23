# 과제: 선착순 쿠폰 발급 시스템 구현

## 1. 배경

이커머스 플랫폼에서 프로모션 이벤트를 진행합니다.

오픈 시각에 수만 명의 사용자가 동시에 쿠폰 발급을 요청하는 상황을 가정합니다.

정해진 수량의 쿠폰이 **정확하게** 발급되어야 하고, 동일 사용자에게 **중복 발급**되어서는 안 됩니다.

단순 CRUD가 아닌, **동시성 제어 · 비동기 처리 · 데이터 정합성** 설계 역량을 평가합니다.

---

## 2. 기술 스택

| 구분             | 기술                      |
| -------------- | ----------------------- |
| Language       | Java 17                 |
| Framework      | Spring Boot 3.x         |
| ORM            | Spring Data JPA         |
| Database       | MySQL 8.0               |
| Cache / 동시성 제어 | Redis 7                 |
| Message Broker | Apache Kafka (KRaft 모드) |
| Build Tool     | Gradle                  |
| 컨테이너           | Docker Compose          |

---

## 3. 요구사항

### 3.1 선착순 쿠폰 발급 API (핵심)

대규모 동시 요청 환경에서 **정확한 수량**만큼만 발급되도록 구현하세요.

| Method | Endpoint | 설명 |
|--------|----------|------|
| GET | `/api/coupons/available` | 발급 가능한 쿠폰 목록 |
| POST | `/api/coupons/{couponId}/issue` | 선착순 쿠폰 발급 요청 |
| GET | `/api/coupons/{couponId}/stock` | 잔여 수량 조회 |

**핵심 제약 조건 (반드시 만족해야 합니다):**

1. **정확한 수량 제어** — 총 발급 수량을 초과하여 발급되면 안 됩니다.
2. **중복 발급 방지** — 동일 사용자에게 같은 쿠폰이 2번 이상 발급되면 안 됩니다.
3. **동시성 안전** — 수천 명이 동시에 요청해도 위 두 조건이 보장되어야 합니다.
4. **빠른 응답** — 사용자에게 최대한 빠르게 응답해야 합니다.

---

### 3.2 쿠폰 사용 API

| Method | Endpoint | 설명 |
|--------|----------|------|
| GET | `/api/coupons/users/{userId}` | 사용자 보유 쿠폰 목록 |
| GET | `/api/coupons/users/{userId}/usable` | 사용 가능한 쿠폰 목록 |
| POST | `/api/coupons/{issuedCouponId}/use` | 쿠폰 사용 |

---

## 4. 도메인 모델

### Coupon

| 필드 | 타입 | 설명 |
|------|------|------|
| id | Long | PK |
| name | String | 쿠폰명 |
| description | String | 설명 |
| couponType | Enum | FIXED_AMOUNT / PERCENTAGE |
| status | Enum | ACTIVE / INACTIVE / EXHAUSTED / EXPIRED |
| discountValue | Integer | 할인 값 |
| minOrderAmount | Integer | 최소 주문 금액 (nullable) |
| maxDiscountAmount | Integer | 최대 할인 금액 (nullable) |
| totalQuantity | Integer | 총 발급 수량 |
| issuedQuantity | Integer | 현재 발급 수량 |
| validDays | Integer | 발급 후 유효 일수 |
| startDate | LocalDateTime | 발급 시작일 |
| endDate | LocalDateTime | 발급 종료일 |

### IssuedCoupon

| 필드        | 타입            | 설명                                  |
| --------- | ------------- | ----------------------------------- |
| id        | Long          | PK                                  |
| coupon    | FK            | Coupon                              |
| userId    | Long          | 발급받은 사용자 ID                         |
| status    | Enum          | ISSUED / USED / EXPIRED / CANCELLED |
| issuedAt  | LocalDateTime | 발급일시                                |
| expiredAt | LocalDateTime | 만료일시                                |
| usedAt    | LocalDateTime | 사용일시 (nullable)                     |

### CouponUsageHistory

| 필드             | 타입            | 설명           |
| -------------- | ------------- | ------------ |
| id             | Long          | PK           |
| issuedCoupon   | FK            | IssuedCoupon |
| userId         | Long          | 사용자 ID       |
| orderId        | Long          | 주문 번호        |
| discountAmount | Integer       | 적용 할인 금액     |
| usedAt         | LocalDateTime | 사용일시         |

---

## 5. 인프라 구성

Docker Compose로 아래 인프라를 구성하세요.

| 서비스 | 포트 | 요구사항 |
|--------|------|---------|
| MySQL 8.0 | 3306 | DB명: `coupon_db`, charset: `utf8mb4` |
| Redis 7 | 6379 | Alpine 이미지 |
| Kafka (KRaft) | 9092 | Zookeeper 없이 KRaft 모드로 구성 |

- 각 서비스는 볼륨 마운트를 통해 데이터가 영속되어야 합니다.
- `docker-compose up -d` 한 번으로 전체 인프라가 실행되어야 합니다.
---

## 6. 예외 처리

아래 커스텀 예외를 구현하고 GlobalExceptionHandler로 처리하세요.

| 예외 | HTTP Status | 설명 |
|------|-------------|------|
| `CouponNotFoundException` | 404 | 쿠폰 없음 |
| `CouponExhaustedException` | 409 | 재고 소진 |
| `DuplicateCouponIssueException` | 409 | 중복 발급 시도 |
| `CouponNotAvailableException` | 400 | 발급 기간 아님 |

**공통 응답 포맷:**

```json
{
  "success": true,
  "data": { ... },
  "message": null
}

{
  "success": false,
  "data": null,
  "message": "이미 발급받은 쿠폰입니다."
}
```

---

## 8. 동시성 테스트

| 시나리오 | 조건 | 기대 결과 |
|---------|------|---------|
| N명 동시 발급 | 1,000명 동시 요청 / 쿠폰 100개 | 정확히 100명만 성공 |
| 동일 사용자 동시 요청 | 1명이 100번 동시 요청 | 정확히 1번만 성공 |
