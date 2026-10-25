package com.example.coupon.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.script.RedisScript;

@Configuration
public class RedisConfig {

    /**
     * 중복 확인 → 재고 차감 → Stream 기록을 한 번에 실행하는 Lua 스크립트.
     * 본문은 resources/scripts/issue-coupon.lua 참고.
     */
    @Bean
    public RedisScript<Long> issueCouponScript() {
        return RedisScript.of(new ClassPathResource("scripts/issue-coupon.lua"), Long.class);
    }
}
