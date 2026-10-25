package com.example.coupon.exception;

import com.example.coupon.api.ApiResponse;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(CouponException.class)
    public ResponseEntity<ApiResponse<Void>> handleCouponException(CouponException e) {
        CouponErrorCode errorCode = e.getErrorCode();
        return ResponseEntity
                .status(errorCode.getHttpStatus())
                .body(ApiResponse.fail(errorCode.getMessage()));
    }

    /** 빈 본문·타입 불일치·userId 누락 — 읽을 수 없는 요청 본문은 모두 같은 400 */
    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<ApiResponse<Void>> handleUnreadableBody(HttpMessageNotReadableException e) {
        return handleCouponException(new CouponException(CouponErrorCode.INVALID_REQUEST));
    }
}
