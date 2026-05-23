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

// ─────────────────────────────────────────────────────────────
// Phase 2 적용 시 추가해야 할 import:
//
// import com.example.coupon.service.CouponRedisService;
// ─────────────────────────────────────────────────────────────

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

@Service
@RequiredArgsConstructor
public class CouponService {

    private final CouponRepository couponRepository;
    private final IssuedCouponRepository issuedCouponRepository;

    // ─────────────────────────────────────────────────────────
    // Phase 2: CouponRedisService 주입 추가
    // TODO: private final CouponRedisService couponRedisService;
    // ─────────────────────────────────────────────────────────

    // ─────────────────────────────────────────────────────────
    // 발급 가능한 쿠폰 목록 조회
    // ─────────────────────────────────────────────────────────
    @Transactional(readOnly = true)
    public Object getAllAvailableCoupons() {
        List<Coupon> ret = couponRepository.findByStatus(Status.ACTIVE);
        return ret;
    }

    // ─────────────────────────────────────────────────────────
    // 선착순 쿠폰 발급 (핵심 로직 — 동시성 제어 여기서 발생)
    //
    // Phase 1 (DB Lock):
    //   findByIdWithLock → SELECT FOR UPDATE → 재고/중복 체크 → INSERT
    //   모두 하나의 @Transactional 안에서 DB Lock으로 직렬화
    //
    // Phase 2 (Redis):
    //   Step 1. 쿠폰 존재/기간 체크 (DB, 캐시 가능)
    //   Step 2. 중복 체크 + 재고 차감 (Redis — CouponRedisService)
    //   Step 3. IssuedCoupon INSERT (DB — 성공한 경우만)
    //
    //   @Transactional 범위가 좁아진다:
    //   Redis 처리(Step 2)는 트랜잭션 밖, DB 쓰기(Step 3)만 트랜잭션 안.
    //   DB Lock이 없으므로 커넥션 풀 고갈 없음.
    //
    // TODO: Phase 2로 전환 시 아래 로직을 수정해라
    //   - findByIdWithLock → findById (Lock 불필요)
    //   - 재고/중복 체크 → couponRedisService.checkAndIssue() 로 교체
    //   - issuedQuantity + 1 제거 (Redis DECR이 대신 처리)
    //   - @Transactional은 issuedCouponRepository.save() 부분에만 유지
    // ─────────────────────────────────────────────────────────
    @Transactional
    public void issueCoupon(Long couponId, Long userId) {
        Optional<Coupon> coupon = couponRepository.findByIdWithLock(couponId);

        // 1. 없으면 에러 처리
        if (coupon.isEmpty()) {
            throw new CouponException(CouponErrorCode.NOT_FOUND);
        }

        // 2. 발급 기간 체크
        LocalDateTime now = LocalDateTime.now();
        if (coupon.get().getStartDate().isAfter(now) || coupon.get().getEndDate().isBefore(now)) {
            throw new CouponException(CouponErrorCode.NOT_AVAILABLE);
        }

        // 3. 재고 체크
        // Phase 2: 이 블록 제거 → couponRedisService.checkAndIssue()가 처리
        if (coupon.get().getIssuedQuantity() >= coupon.get().getTotalQuantity()) {
            throw new CouponException(CouponErrorCode.EXHAUSTED);
        }

        // 4. 중복 발급
        // Phase 2: 이 블록 제거 → couponRedisService.checkAndIssue()가 처리
        if (issuedCouponRepository.existsByCoupon_IdAndUserId(couponId, userId)) {
            throw new CouponException(CouponErrorCode.DUPLICATED);
        }

        // 5. 발급 수량 증가
        // Phase 2: 이 줄 제거 → Redis DECR이 대신 처리, DB 동기화는 별도
        coupon.get().setIssuedQuantity(coupon.get().getIssuedQuantity() + 1);

        // 6. issuedCoupon 생성
        // Phase 2: 이 블록은 그대로 유지 (성공한 경우만 여기까지 도달)
        IssuedCoupon issuedCoupon = new IssuedCoupon();
        issuedCoupon.setCoupon(coupon.get());
        issuedCoupon.setUserId(userId);
        issuedCoupon.setStatus(com.example.coupon.domain.issuedCoupon.Status.ISSUED);
        issuedCoupon.setIssuedAt(now);
        issuedCoupon.setExpiredAt(now.plusDays(coupon.get().getValidDays()));
        issuedCouponRepository.save(issuedCoupon);
    }

    // ─────────────────────────────────────────────────────────
    // 잔여 수량 조회
    //
    // Phase 2: Cache-aside 패턴으로 변경
    //   1. couponRedisService.getStock() 호출
    //   2. -1이면 Redis에 키 없음 → DB 조회 → Redis 초기화 후 반환
    //   3. 0 이상이면 Redis 값 바로 반환
    //
    // TODO: Phase 2 전환 시 Redis 우선 조회로 변경해라
    // ─────────────────────────────────────────────────────────
    @Transactional(readOnly = true)
    public Object getStock(Long couponId) {
        Optional<Coupon> coupon = couponRepository.findById(couponId);
        if (coupon.isEmpty()) {
            throw new CouponException(CouponErrorCode.NOT_FOUND);
        }

        return coupon.get().getTotalQuantity() - coupon.get().getIssuedQuantity();
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
