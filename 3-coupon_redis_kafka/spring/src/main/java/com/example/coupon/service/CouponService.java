package com.example.coupon.service;

import com.example.coupon.domain.coupon.Coupon;
import com.example.coupon.domain.coupon.Status;
import com.example.coupon.domain.issuedCoupon.IssuedCoupon;
import com.example.coupon.exception.CouponErrorCode;
import com.example.coupon.exception.CouponException;
import com.example.coupon.repository.CouponRepository;
import com.example.coupon.repository.IssuedCouponRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

// TODO: CouponKafkaProducer를 import 하세요.

@Service
@RequiredArgsConstructor
public class CouponService {

    private final CouponRepository couponRepository;
    private final IssuedCouponRepository issuedCouponRepository;
    private final CouponRedisService couponRedisService;
    private final TransactionTemplate transactionTemplate;
    private final CouponKafkaProducer couponKafkaProducer;

    // ─────────────────────────────────────────────────────────
    // 발급 가능한 쿠폰 목록 조회
    // ─────────────────────────────────────────────────────────
    @Transactional(readOnly = true)
    public Object getAllAvailableCoupons() {
        List<Coupon> ret = couponRepository.findByStatus(Status.ACTIVE);
        return ret;
    }

    // ─────────────────────────────────────────────────────────
    // 선착순 쿠폰 발급
    //
    // Phase 3 흐름:
    //   Step 1. 기간 체크 — Redis (DB 커넥션 없음, 1000개 전부 처리)
    //   Step 2. 중복 체크 + 재고 차감 — Redis 원자적 연산 (900개 여기서 탈락)
    //   Step 3. Kafka 이벤트 발행 — 성공한 100개만 도달, 즉시 반환
    //           DB INSERT는 CouponKafkaConsumer가 비동기로 처리
    //
    // Phase 2와의 차이:
    //   Phase 2 Step 3 → TransactionTemplate으로 MySQL INSERT (동기)
    //   Phase 3 Step 3 → Kafka 발행 후 즉시 반환 (비동기)
    //   덕분에 Controller에서 200 대신 202 Accepted를 반환할 수 있다.
    //
    // TransactionTemplate을 제거해도 되는가?
    //   DB 쓰기가 Consumer로 이동했으므로 이 메서드에는 트랜잭션이 필요 없다.
    //   TransactionTemplate 필드는 제거하거나 unused 상태로 둬도 된다.
    // ─────────────────────────────────────────────────────────
    public void issueCoupon(Long couponId, Long userId) {
        // Step 1. 기간 체크 (Redis) — Phase 2와 동일, 변경 없음
        couponRedisService.validatePeriod(couponId);

        // Step 2. 중복 체크 + 재고 차감 (Redis) — Phase 2와 동일, 변경 없음
        couponRedisService.checkAndIssue(couponId, userId);

        //   Phase 2에서는 DB INSERT 실패 시 Redis를 수동으로 롤백했지만,
        //   Phase 3에서는 Kafka 발행 실패 시 rollback()이 의미 없습니다.
        //   (Kafka 발행 자체가 실패하면 유저에게 예외가 전파되므로 Redis에 이미 차감된
        //    재고가 남게 됩니다. 이 문제는 Outbox Pattern으로 해결하지만 이 프로젝트에서는
        //    학습 목적으로 허용합니다. phase3-study.md 7장 참고)
        couponKafkaProducer.publish(couponId, userId);
    }

    // ─────────────────────────────────────────────────────────
    // 잔여 수량 조회
    //
    // Phase 2: Cache-aside 패턴으로 변경
    //   1. couponRedisService.getStock() 호출
    //   2. -1이면 Redis에 키 없음 → DB 조회 → Redis 초기화 후 반환
    //   3. 0 이상이면 Redis 값 바로 반환
    // ─────────────────────────────────────────────────────────
    @Transactional(readOnly = true)
    public Object getStock(Long couponId) {
        int stock = couponRedisService.getStock(couponId);

        if (stock == -1) {
            Optional<Coupon> coupon = couponRepository.findById(couponId);
            if (coupon.isEmpty()) {
                throw new CouponException(CouponErrorCode.NOT_FOUND);
            }
            return coupon.get().getTotalQuantity() - coupon.get().getIssuedQuantity();
        }
        return stock;
    }

    // ─────────────────────────────────────────────────────────
    // 사용자 보유 쿠폰 전체 목록
    // ─────────────────────────────────────────────────────────
    @Transactional(readOnly = true)
    public Object getUserCoupons(Long userId) {
        return issuedCouponRepository.findByUserId(userId);
    }

    // ─────────────────────────────────────────────────────────
    // 사용 가능한 쿠폰 목록 (status = ISSUED)
    // ─────────────────────────────────────────────────────────
    @Transactional(readOnly = true)
    public Object getUserUsableCoupons(Long userId) {
        return issuedCouponRepository.findByUserIdAndStatus(userId, com.example.coupon.domain.issuedCoupon.Status.ISSUED);
    }

    // ─────────────────────────────────────────────────────────
    // Redis 재고 초기화
    //   DB의 totalQuantity로 Redis stock 키를 세팅한다.
    //   테스트 리셋 또는 쿠폰 생성 후 최초 초기화 시 사용.
    // ─────────────────────────────────────────────────────────
    public void initStock(Long couponId) {
        Coupon coupon = couponRepository.findById(couponId)
                .orElseThrow(() -> new CouponException(CouponErrorCode.NOT_FOUND));
        couponRedisService.initStock(couponId, coupon.getTotalQuantity(), coupon.getStartDate(), coupon.getEndDate());
    }

    // ─────────────────────────────────────────────────────────
    // 쿠폰 사용 처리
    // ─────────────────────────────────────────────────────────
    @Transactional
    public void useCoupon(Long issuedCouponId) {
        Optional<IssuedCoupon> coupon = issuedCouponRepository.findById(issuedCouponId);

        // 1. 쿠폰 확인
        if (coupon.isEmpty()) {
            throw new CouponException(CouponErrorCode.NOT_FOUND);
        }

        // 2. 사용 가능 status 확인
        if (coupon.get().getStatus() != com.example.coupon.domain.issuedCoupon.Status.ISSUED) {
            throw new CouponException(CouponErrorCode.NOT_AVAILABLE);
        }

        // 3. 만료 여부 확인
        if (coupon.get().getExpiredAt().isBefore(LocalDateTime.now())) {
            throw new CouponException(CouponErrorCode.EXPIRATION);
        }

        // 4. 상태 변경
        coupon.get().setStatus(com.example.coupon.domain.issuedCoupon.Status.USED);
        coupon.get().setUsedAt(LocalDateTime.now());
    }
}
