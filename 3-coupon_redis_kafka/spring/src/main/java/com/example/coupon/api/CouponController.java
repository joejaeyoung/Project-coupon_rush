package com.example.coupon.api;

import com.example.coupon.service.CouponService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@CrossOrigin(origins = "*")
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
    //
    //   Phase 2: DB INSERT까지 완료한 후 200 OK 반환
    //   Phase 3: Kafka에 이벤트 발행만 하고 즉시 202 Accepted 반환
    //            DB INSERT는 Consumer가 백그라운드에서 처리
    //
    //   202 Accepted의 의미:
    //     "요청은 받았고 유효하지만, 처리가 아직 완료되지 않았다"는 HTTP 표준 상태 코드.
    //     쿠폰 발급이 확정된 건 아니고, Kafka Consumer가 성공적으로 INSERT해야 최종 확정.
    // ─────────────────────────────────────────────────────────
    @PostMapping("/{couponId}/issue")
    public ResponseEntity<ApiResponse<?>> issueCoupon(
            @PathVariable Long couponId,
            @RequestParam(required = false) Long userId,
            @RequestBody(required = false) java.util.Map<String, Long> body
    ) {
        if (userId == null && body != null) userId = body.get("userId");
        couponService.issueCoupon(couponId, userId);
        return ResponseEntity.accepted().body(ApiResponse.ok("쿠폰 발급 요청이 접수되었습니다."));
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
    // POST /api/coupons/{couponId}/stock/init
    // Redis 재고 초기화 (DB totalQuantity 기준)
    // ─────────────────────────────────────────────────────────
    @PostMapping("/{couponId}/stock/init")
    public ResponseEntity<ApiResponse<?>> initStock(@PathVariable Long couponId) {
        couponService.initStock(couponId);
        return ResponseEntity.ok(ApiResponse.ok("재고가 초기화되었습니다."));
    }

    // ─────────────────────────────────────────────────────────
    // POST /api/coupons/{issuedCouponId}/use
    // 쿠폰 사용 처리
    // ─────────────────────────────────────────────────────────
    @PostMapping("/{issuedCouponId}/use")
    public ResponseEntity<ApiResponse<?>> useCoupon(@PathVariable Long issuedCouponId) {
        couponService.useCoupon(issuedCouponId);
        return ResponseEntity.ok(ApiResponse.ok("쿠폰이 사용되었습니다."));
    }
}
