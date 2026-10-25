package com.example.coupon.repository;

import com.example.coupon.domain.issuedcoupon.IssuedCoupon;
import com.example.coupon.domain.issuedcoupon.IssuedCouponStatus;
import org.springframework.data.jpa.repository.EntityGraph;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface IssuedCouponRepository extends JpaRepository<IssuedCoupon, Long> {

    // 응답 DTO가 coupon.name을 읽는다 — join fetch로 쿠폰 수만큼의 추가 SELECT(N+1)를 막는다
    @EntityGraph(attributePaths = "coupon")
    List<IssuedCoupon> findByUserId(Long userId);

    @EntityGraph(attributePaths = "coupon")
    List<IssuedCoupon> findByUserIdAndStatus(Long userId, IssuedCouponStatus status);

    /** Consumer 멱등성 검사: 같은 이벤트가 두 번 와도 한 번만 저장한다 */
    boolean existsByCouponIdAndUserId(Long couponId, Long userId);
}
