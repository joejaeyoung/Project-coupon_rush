package com.example.couponlab;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication
public class CouponLabApplication {
    public static void main(String[] args) { SpringApplication.run(CouponLabApplication.class, args); }
}
