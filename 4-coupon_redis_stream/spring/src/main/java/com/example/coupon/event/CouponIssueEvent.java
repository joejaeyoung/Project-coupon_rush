package com.example.coupon.event;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * 쿠폰 발급 이벤트.
 *
 * Redis Stream 레코드(issue-coupon.lua의 XADD) → CouponStreamRelay → Kafka "coupon-issue" → CouponKafkaConsumer
 */
@Getter
@NoArgsConstructor  // Kafka JSON 역직렬화용
@AllArgsConstructor
public class CouponIssueEvent {

    private Long couponId;
    private Long userId;
    private String issuedAt;

    /** Stream 레코드 필드에서 이벤트를 복원한다. 필드명은 issue-coupon.lua의 XADD 인자와 맞춘다. */
    public static CouponIssueEvent from(Map<Object, Object> fields) {
        return new CouponIssueEvent(
                Long.valueOf(fields.get("couponId").toString()),
                Long.valueOf(fields.get("userId").toString()),
                fields.get("issuedAt").toString()
        );
    }
}
