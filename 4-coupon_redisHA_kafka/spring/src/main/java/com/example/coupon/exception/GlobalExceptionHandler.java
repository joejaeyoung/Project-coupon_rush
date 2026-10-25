package com.example.coupon.exception;

import com.example.coupon.api.ApiResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@Slf4j
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
    public ResponseEntity<ApiResponse<Void>> handleUnreadableBody() {
        return handleCouponException(new CouponException(CouponErrorCode.INVALID_REQUEST));
    }

    /** Redis·MySQL에 닿지 못하거나 명령이 제때 끝나지 않음 — Redis failover 동안 몇 초간 발생한다 */
    @ExceptionHandler(DataAccessException.class)
    public ResponseEntity<ApiResponse<Void>> handleStoreUnavailable(DataAccessException e) {
        log.warn("저장소 접근 실패 — {}", e.getMostSpecificCause().getMessage());
        return handleCouponException(new CouponException(CouponErrorCode.STORE_UNAVAILABLE, e));
    }
}
