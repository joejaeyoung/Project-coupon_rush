package com.example.coupon.repository;

import com.example.coupon.domain.Coupon;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface CouponRepository extends JpaRepository<Coupon, Long> {

    /** 총량 안에서만 1 증가. 0을 돌려주면 DB 기준 재고 초과(또는 쿠폰 없음)다 */
    @Modifying
    @Query("UPDATE Coupon c SET c.issuedQuantity = c.issuedQuantity + 1 WHERE c.id = :id AND c.issuedQuantity < c.totalQuantity")
    int incrementIssuedQuantity(@Param("id") Long id);
}
