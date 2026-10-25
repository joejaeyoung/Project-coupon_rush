package com.example.coupon.exception;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;

@Getter
@RequiredArgsConstructor
public enum CouponErrorCode {

    INVALID_REQUEST("userId는 필수입니다.", HttpStatus.BAD_REQUEST),
    NOT_AVAILABLE("발급 기간이 아닙니다.", HttpStatus.BAD_REQUEST),
    NOT_FOUND("쿠폰을 찾을 수 없습니다.", HttpStatus.NOT_FOUND),
    EXHAUSTED("쿠폰이 모두 소진되었습니다.", HttpStatus.CONFLICT),
    DUPLICATED("이미 발급받은 쿠폰입니다.", HttpStatus.CONFLICT),
    EXPIRED("만료된 쿠폰입니다.", HttpStatus.NOT_FOUND);

    private final String message;
    private final HttpStatus httpStatus;
}
