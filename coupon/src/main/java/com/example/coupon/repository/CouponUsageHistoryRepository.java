package com.example.coupon.repository;

import com.example.coupon.domain.CouponUsageHistory;
import org.springframework.data.jpa.repository.JpaRepository;

public interface CouponUsageHistoryRepository extends JpaRepository<CouponUsageHistory, Long> {
    // Phase 1에서는 쿠폰 사용 이력 저장까지만 구현합니다
    // 별도 조회 메서드는 필요 시 추가하세요
}
