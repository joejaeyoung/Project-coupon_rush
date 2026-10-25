package com.example.coupon.api;

import java.util.Objects;

public record CouponIssueRequest(Long userId) {

    public CouponIssueRequest {
        Objects.requireNonNull(userId, "userId"); // 역직렬화 단계에서 막혀 HttpMessageNotReadableException → 400
    }
}
