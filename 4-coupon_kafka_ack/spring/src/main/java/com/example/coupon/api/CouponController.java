package com.example.coupon.api;

import com.example.coupon.domain.coupon.Coupon;
import com.example.coupon.service.CouponService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@CrossOrigin(origins = "*") // load-test.html(file://)에서 호출
@RestController
@RequestMapping("/api/coupons")
@RequiredArgsConstructor
public class CouponController {

    private final CouponService couponService;

    @GetMapping("/available")
    public ApiResponse<List<Coupon>> getAvailableCoupons() {
        return ApiResponse.ok(couponService.getAvailableCoupons());
    }

    /** Redis 판정 후 Kafka 브로커 ACK까지 받으면 202 Accepted. DB 저장은 Consumer가 비동기로 처리한다. */
    @PostMapping("/{couponId}/issue")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public ApiResponse<String> issueCoupon(@PathVariable Long couponId, @RequestBody CouponIssueRequest request) {
        couponService.issueCoupon(couponId, request.userId());
        return ApiResponse.ok("쿠폰 발급 요청이 접수되었습니다.");
    }

    @GetMapping("/{couponId}/stock")
    public ApiResponse<Integer> getStock(@PathVariable Long couponId) {
        return ApiResponse.ok(couponService.getStock(couponId));
    }

    @GetMapping("/users/{userId}")
    public ApiResponse<List<IssuedCouponResponse>> getUserCoupons(@PathVariable Long userId) {
        return ApiResponse.ok(couponService.getUserCoupons(userId));
    }

    @GetMapping("/users/{userId}/usable")
    public ApiResponse<List<IssuedCouponResponse>> getUserUsableCoupons(@PathVariable Long userId) {
        return ApiResponse.ok(couponService.getUserUsableCoupons(userId));
    }

    /** Redis 재고·기간 초기화 (DB totalQuantity 기준). 쿠폰 생성 직후 또는 테스트 리셋 시 호출한다. */
    @PostMapping("/{couponId}/stock/init")
    public ApiResponse<String> initStock(@PathVariable Long couponId) {
        couponService.initStock(couponId);
        return ApiResponse.ok("재고가 초기화되었습니다.");
    }

    @PostMapping("/{issuedCouponId}/use")
    public ApiResponse<String> useCoupon(@PathVariable Long issuedCouponId) {
        couponService.useCoupon(issuedCouponId);
        return ApiResponse.ok("쿠폰이 사용되었습니다.");
    }
}
