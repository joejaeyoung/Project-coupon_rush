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

    // enum 이름은 서버 내부 분류다. HTTP 본문에는 code 필드 없이 한국어 message만 담는다.
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

    /**
     * HTTP 처리 중 발생한 저장소 예외를 503으로 변환한다(접근 실패 외 DataAccessException도 포함).
     * Kafka Consumer의 예외는 MVC Advice가 아닌 리스너 컨테이너의 오류 처리기로 전달된다.
     */
    @ExceptionHandler(DataAccessException.class)
    public ResponseEntity<ApiResponse<Void>> handleStoreUnavailable(DataAccessException e) {
        log.warn("저장소 접근 실패 — {}", e.getMostSpecificCause().getMessage());
        return handleCouponException(new CouponException(CouponErrorCode.STORE_UNAVAILABLE, e));
    }
}
