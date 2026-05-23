package com.example.coupon.exception;

import lombok.Getter;
import org.springframework.http.HttpStatus;

@Getter
public enum CouponErrorCode {
    NOT_AVAILABLE("발급 기간 아님", HttpStatus.BAD_REQUEST),
    NOT_FOUND("쿠폰 없음", HttpStatus.NOT_FOUND),
    EXHAUSTED("재고 소진", HttpStatus.CONFLICT),
    DUPLICATED("중복 발급 시도", HttpStatus.CONFLICT),
    EXPIRATION("쿠폰 만료", HttpStatus.NOT_FOUND);

    private HttpStatus httpStatus;
    private String message;
    CouponErrorCode(String message, HttpStatus httpStatus) {
        this.message = message;
        this.httpStatus = httpStatus;
    }
}
