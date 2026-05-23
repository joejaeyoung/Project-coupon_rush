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

@Service
@RequiredArgsConstructor
public class CouponService {

    private final CouponRepository couponRepository;
    private final IssuedCouponRepository issuedCouponRepository;
    private final CouponRedisService couponRedisService;
    private final TransactionTemplate transactionTemplate;


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
    // Phase 2 흐름:
    //   Step 1. 쿠폰 존재/기간 체크 — DB 조회 후 커넥션 즉시 반납
    //   Step 2. 중복 체크 + 재고 차감 — Redis 원자적 연산 (900개는 여기서 탈락)
    //   Step 3. DB 쓰기 — TransactionTemplate으로 범위 최소화 (성공 100개만 도달)
    //
    // @Transactional을 메서드 전체에 걸지 않는 이유:
    //   전체에 걸면 1000개 요청 모두 Step 1에서 DB 커넥션을 잡고
    //   Step 2(Redis)가 끝날 때까지 커넥션을 점유 → 커넥션 풀 고갈
    // ─────────────────────────────────────────────────────────
    public void issueCoupon(Long couponId, Long userId) {
        // Step 1. 기간 체크 (Redis — DB 커넥션 없음, 1000개 전부 여기서 처리)
        couponRedisService.validatePeriod(couponId);

        // Step 2. 중복 체크 + 재고 차감 (Redis 원자적, 900개 여기서 탈락)
        couponRedisService.checkAndIssue(couponId, userId);

        // Step 3. DB 쓰기 — 성공한 100개만 도달
        final LocalDateTime now = LocalDateTime.now();
        try {
            transactionTemplate.execute(status -> {
                Coupon coupon = couponRepository.findById(couponId)
                        .orElseThrow(() -> new CouponException(CouponErrorCode.NOT_FOUND));
                couponRepository.incrementIssuedQuantity(couponId);
                IssuedCoupon issuedCoupon = new IssuedCoupon();
                issuedCoupon.setCoupon(coupon);
                issuedCoupon.setUserId(userId);
                issuedCoupon.setStatus(com.example.coupon.domain.issuedCoupon.Status.ISSUED);
                issuedCoupon.setIssuedAt(now);
                issuedCoupon.setExpiredAt(now.plusDays(coupon.getValidDays()));
                issuedCouponRepository.save(issuedCoupon);
                return null;
            });
        } catch (Exception e) {
            couponRedisService.rollback(couponId, userId);
            throw e;
        }
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
