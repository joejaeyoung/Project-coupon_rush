package com.example.coupon.repository;

import com.example.coupon.domain.IssuedCoupon;
import org.springframework.data.jpa.repository.JpaRepository;

public interface IssuedCouponRepository extends JpaRepository<IssuedCoupon, Long> {

    /** Consumer 멱등성 검사: 같은 이벤트가 두 번 와도 한 번만 저장한다 */
    boolean existsByCouponIdAndUserId(Long couponId, Long userId);
}
