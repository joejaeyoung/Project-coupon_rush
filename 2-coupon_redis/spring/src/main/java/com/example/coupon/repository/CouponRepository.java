package com.example.coupon.repository;

import com.example.coupon.domain.coupon.Coupon;
import com.example.coupon.domain.coupon.Status;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;
import java.util.Optional;

public interface CouponRepository extends JpaRepository<Coupon, Long> {

    // 발급 가능한 쿠폰 목록 (status = ACTIVE)
    @Query("SELECT c FROM Coupon c WHERE c.status = :status")
    List<Coupon> findByStatus(@Param("status") Status status);

    // ─────────────────────────────────────────────────────────
    // 선착순 발급용 Pessimistic Lock 조회
    // @Lock(LockModeType.PESSIMISTIC_WRITE) → SELECT ... FOR UPDATE
    // 같은 쿠폰에 동시 접근 시 하나씩 순서대로 처리되도록 Row Lock을 건다
    // ─────────────────────────────────────────────────────────
     @Lock(LockModeType.PESSIMISTIC_WRITE)
     @Query("SELECT c FROM Coupon c WHERE c.id = :id")
     Optional<Coupon> findByIdWithLock(@Param("id") Long id);

    // 잔여 수량 조회: totalQuantity - issuedQuantity
    @Query("SELECT c.totalQuantity - c.issuedQuantity FROM Coupon c WHERE c.id = :id")
    Integer getRemainingQuantity(@Param("id") Long id);

    @Modifying(clearAutomatically = true)
    @Query("UPDATE Coupon c SET c.issuedQuantity = c.issuedQuantity + 1 WHERE c.id = :id")
    void incrementIssuedQuantity(@Param("id") Long id);
}
