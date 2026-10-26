package com.example.coupon.api;

import com.example.coupon.service.CouponService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/coupons")
@RequiredArgsConstructor
public class CouponController {

    private final CouponService couponService;

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

    /** DB totalQuantity·기간을 Redis에 올린다. 쿠폰 생성 직후 또는 테스트 리셋 시 호출한다. */
    @PostMapping("/{couponId}/stock/init")
    public ApiResponse<String> initStock(@PathVariable Long couponId) {
        couponService.initStock(couponId);
        return ApiResponse.ok("재고가 초기화되었습니다.");
    }
}
