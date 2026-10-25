package com.example.coupon.domain;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Entity
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class Coupon {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private Integer totalQuantity;
    private Integer issuedQuantity = 0; // Kafka Consumer의 조건부 UPDATE만 올린다 (빌더에서 제외)
    private LocalDateTime startDate;
    private LocalDateTime endDate;

    @Builder
    public Coupon(Integer totalQuantity, LocalDateTime startDate, LocalDateTime endDate) {
        this.totalQuantity = totalQuantity;
        this.startDate = startDate;
        this.endDate = endDate;
    }
}
