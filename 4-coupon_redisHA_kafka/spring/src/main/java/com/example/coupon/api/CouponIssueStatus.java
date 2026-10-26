package com.example.coupon.api;

/** DB 발급 기록의 조회 결과. NOT_CONFIRMED는 처리 중과 최종 실패를 구분하지 않는다. */
public enum CouponIssueStatus {
    ISSUED,
    NOT_CONFIRMED
}
