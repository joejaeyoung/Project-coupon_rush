package com.example.coupon.repository;

import com.example.coupon.domain.issuedcoupon.IssuedCoupon;
import com.example.coupon.domain.issuedcoupon.IssuedCouponStatus;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface IssuedCouponRepository extends JpaRepository<IssuedCoupon, Long> {

    List<IssuedCoupon> findByUserId(Long userId);

    List<IssuedCoupon> findByUserIdAndStatus(Long userId, IssuedCouponStatus status);

    /** Consumer 멱등성 검사: 같은 이벤트가 두 번 와도 한 번만 저장한다 */
    boolean existsByCouponIdAndUserId(Long couponId, Long userId);
}
