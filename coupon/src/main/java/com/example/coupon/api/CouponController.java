package com.example.coupon.api;

import com.example.coupon.service.CouponService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/coupons")
@RequiredArgsConstructor
public class CouponController {

    private final CouponService couponService;

    // ─────────────────────────────────────────────────────────
    // GET /api/coupons/available
    // 발급 가능한 쿠폰 목록 반환
    // ─────────────────────────────────────────────────────────
    @GetMapping("/available")
    public ResponseEntity<ApiResponse<?>> getAvailableCoupons() {
        return ResponseEntity.ok(ApiResponse.ok(couponService.getAllAvailableCoupons()));
    }

    // ─────────────────────────────────────────────────────────
    // POST /api/coupons/{couponId}/issue
    // 선착순 쿠폰 발급 요청 (핵심 API)
    // ─────────────────────────────────────────────────────────
    @PostMapping("/{couponId}/issue")
    public ResponseEntity<ApiResponse<?>> issueCoupon(
            @PathVariable Long couponId,
            @RequestParam Long userId
    ) {
        couponService.issueCoupon(couponId, userId);
        return ResponseEntity.ok(ApiResponse.ok(""));
    }

    // ─────────────────────────────────────────────────────────
    // GET /api/coupons/{couponId}/stock
    // 쿠폰 잔여 수량 조회
    // ─────────────────────────────────────────────────────────
    @GetMapping("/{couponId}/stock")
    public ResponseEntity<ApiResponse<?>> getStock(@PathVariable Long couponId) {
        Object stock = couponService.getStock(couponId);
        return ResponseEntity.ok(ApiResponse.ok(stock));
    }

    // ─────────────────────────────────────────────────────────
    // GET /api/coupons/users/{userId}
    // 사용자가 보유한 쿠폰 전체 목록
    // ─────────────────────────────────────────────────────────
    @GetMapping("/users/{userId}")
    public ResponseEntity<ApiResponse<?>> getUserCoupons(@PathVariable Long userId) {
        Object coupons = couponService.getUserCoupons(userId);
        return ResponseEntity.ok(ApiResponse.ok(coupons));
    }

    // ─────────────────────────────────────────────────────────
    // GET /api/coupons/users/{userId}/usable
    // 사용자가 실제로 사용 가능한 쿠폰 목록 (status = ISSUED)
    // ─────────────────────────────────────────────────────────
    @GetMapping("/users/{userId}/usable")
    public ResponseEntity<ApiResponse<?>> getUserUsableCoupons(@PathVariable Long userId) {
        Object userUsableCoupons = couponService.getUserUsableCoupons(userId);
        return ResponseEntity.ok(ApiResponse.ok(userUsableCoupons));
    }

    // ─────────────────────────────────────────────────────────
    // POST /api/coupons/{issuedCouponId}/use
    // 쿠폰 사용 처리
    // ─────────────────────────────────────────────────────────
    @PostMapping("/{issuedCouponId}/use")
    public ResponseEntity<ApiResponse<?>> useCoupon(@PathVariable Long issuedCouponId) {
        couponService.useCoupon(issuedCouponId);
        return null;
    }
}
