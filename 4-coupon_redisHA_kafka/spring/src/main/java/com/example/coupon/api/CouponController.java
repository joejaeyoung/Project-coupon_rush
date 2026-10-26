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

    /**
     * MySQL 발급 기록으로 최종 저장 여부를 확인한다. 기록이 없으면 처리 중인지 실패했는지 구분할 수 없다.
     * 따라서 NOT_CONFIRMED는 실패 확정 상태가 아니다.
     */
    @GetMapping("/{couponId}/issues/users/{userId}")
    public ApiResponse<CouponIssueStatus> getIssueStatus(@PathVariable Long couponId, @PathVariable Long userId) {
        CouponIssueStatus status = couponService.isIssued(couponId, userId)
                ? CouponIssueStatus.ISSUED : CouponIssueStatus.NOT_CONFIRMED;

        return ApiResponse.ok(status);
    }

    /** Redis의 예약 가능 수량이다. Consumer가 저장한 최종 발급 수량을 조회하는 API가 아니다. */
    @GetMapping("/{couponId}/stock")
    public ApiResponse<Integer> getStock(@PathVariable Long couponId) {
        return ApiResponse.ok(couponService.getStock(couponId));
    }

    /** DB 총량·기간을 Redis에 올린다. users 삭제·DB 발급 기록 초기화는 별도이며 실험 준비 시 호출한다. */
    @PostMapping("/{couponId}/stock/init")
    public ApiResponse<String> initStock(@PathVariable Long couponId) {
        couponService.initStock(couponId);

        return ApiResponse.ok("재고가 초기화되었습니다.");
    }
}
