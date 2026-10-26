# AOP 프록시

## AOP란?

**AOP(Aspect-Oriented Programming, 관점 지향 프로그래밍)**는 핵심 비즈니스 로직과 공통 부가 로직(트랜잭션, 로깅, 캐싱 등)을 분리하는 프로그래밍 패러다임이다.

Spring에서 AOP는 **프록시 패턴**으로 구현된다.

---

## 문제: 공통 코드가 여기저기 흩어짐

트랜잭션 시작/커밋은 모든 서비스 메서드에 필요한데, 직접 쓰면 이렇게 된다:

```java
public void issueCoupon() {
    transaction.begin();      // 매번 반복
    try {
        coupon.issue();       // 실제 비즈니스 로직
        transaction.commit();
    } catch (Exception e) {
        transaction.rollback();
    }
}

public void useCoupon() {
    transaction.begin();      // 또 반복
    try {
        coupon.use();
        transaction.commit();
    } catch (Exception e) {
        transaction.rollback();
    }
}
```

핵심 로직(쿠폰 발급)과 부가 로직(트랜잭션)이 뒤섞여서 지저분하다.

---

## 해결: 프록시 패턴

Spring은 `@Transactional`이 붙은 클래스를 발견하면 원본 클래스를 직접 쓰지 않고, 그것을 감싸는 **프록시 객체**를 대신 만든다.

```
[Controller]
     ↓ couponService.issueCoupon() 호출
[CouponService 프록시]  ← Spring이 몰래 만든 가짜 객체
     ↓ 트랜잭션 시작
[CouponService 실제 객체]
     ↓ 비즈니스 로직 실행
[CouponService 프록시]
     ↓ 트랜잭션 커밋 (or 롤백)
```

```java
// 내가 작성한 코드
@Service
public class CouponService {
    @Transactional
    public void issueCoupon() {
        coupon.issue();  // 비즈니스 로직만 작성
    }
}

// Spring이 내부적으로 만드는 것 (의사 코드)
public class CouponService$$SpringProxy extends CouponService {
    @Override
    public void issueCoupon() {
        transaction.begin();
        try {
            super.issueCoupon();   // 내 실제 코드 호출
            transaction.commit();
        } catch (Exception e) {
            transaction.rollback();
        }
    }
}
```

Controller가 `CouponService`를 주입받을 때 Spring은 진짜 `CouponService`가 아니라 이 **프록시 객체**를 준다. Controller는 프록시인지 모르고 그냥 쓰는데, 실제로는 앞뒤로 트랜잭션 코드가 자동으로 실행된다.

---

## 이것 때문에 생기는 제약

### 같은 클래스 내부 호출은 프록시를 거치지 않는다

```java
@Service
public class CouponService {

    public void outer() {
        inner();  // this.inner() — 프록시가 아닌 자기 자신을 직접 호출
    }

    @Transactional
    public void inner() {
        // @Transactional이 있어도 트랜잭션이 시작되지 않음 ⚠️
    }
}
```

`outer()`를 호출하면 `프록시 → outer() → this.inner()` 경로로 가기 때문에 `inner()`의 `@Transactional`이 무시된다. 트랜잭션이 필요한 메서드는 **항상 외부에서 호출**되어야 한다.

### private 메서드에는 적용되지 않는다

프록시는 상속(또는 인터페이스 구현) 방식으로 만들어지기 때문에 `private` 메서드를 오버라이드할 수 없다. `@Transactional`을 `private` 메서드에 붙여도 무시된다.

---

## AOP의 핵심 용어

| 용어 | 설명 | 예시 |
|------|------|------|
| **Aspect** | 부가 기능 모듈 | 트랜잭션 처리 |
| **Advice** | 실제로 끼워 넣는 코드 | `transaction.begin()`, `commit()` |
| **Pointcut** | Advice를 적용할 지점 선언 | `@Transactional`이 붙은 메서드 |
| **JoinPoint** | Advice가 실제로 실행되는 시점 | 메서드 호출 시점 |
| **Weaving** | Pointcut에 Advice를 끼워 넣는 과정 | Spring이 프록시 객체를 만드는 것 |

---

## Spring에서 AOP로 동작하는 것들

트랜잭션은 AOP의 대표 예시일 뿐이다. 같은 원리로 동작하는 것들:

| 어노테이션 | 끼워 넣는 동작 |
|-----------|-------------|
| `@Transactional` | 트랜잭션 시작 / 커밋 / 롤백 |
| `@Cacheable` | 캐시 조회 → 없으면 실행 → 캐시 저장 |
| `@Async` | 별도 스레드에서 실행 |
| `@Scheduled` | 일정 주기로 실행 |

---

## 커스텀 AOP 작성 예시

계획에만 있는 Read Replica 단계에서 읽기/쓰기 DataSource를 분리한다면 직접 AOP를 작성한다 (현재 Phase 4 `4-coupon_redisHA_kafka`에는 없다):

```java
@Aspect
@Component
public class ReadOnlyRoutingAspect {

    @Around("@annotation(transactional)")
    public Object route(ProceedingJoinPoint pjp, Transactional transactional) throws Throwable {
        if (transactional.readOnly()) {
            DataSourceContextHolder.set("replica");  // 읽기 → Replica DB
        } else {
            DataSourceContextHolder.set("primary");  // 쓰기 → Primary DB
        }
        try {
            return pjp.proceed();  // 실제 메서드 실행
        } finally {
            DataSourceContextHolder.clear();
        }
    }
}
```

`@Transactional(readOnly = true)`가 붙은 메서드는 Replica DB로, 아닌 메서드는 Primary DB로 자동 라우팅된다.
