package com.example.coupon.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class CouponKafkaConfiguration {
    @Bean
    NewTopic couponIssueTopic(@Value("${coupon.kafka.topic:coupon-issue}") String topic,
                             @Value("${coupon.kafka.partitions:1}") int partitions) {
        // These experiments use a single broker. Partition changes use a fresh topic.
        return TopicBuilder.name(topic).partitions(partitions).replicas(1).build();
    }
}
