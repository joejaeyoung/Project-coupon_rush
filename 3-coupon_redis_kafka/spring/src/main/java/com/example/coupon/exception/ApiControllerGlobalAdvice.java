package com.example.coupon.exception;

import com.example.coupon.api.ApiResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@Slf4j
@RestControllerAdvice
public class ApiControllerGlobalAdvice {

    @ExceptionHandler(CouponException.class)
    public ResponseEntity<ApiResponse<Void>> couponException(CouponException e) {
        CouponErrorCode errorCode = e.getErrorCode();
        return ResponseEntity
                .status(errorCode.getHttpStatus())
                .body(ApiResponse.fail(errorCode.getMessage()));
    }
}

