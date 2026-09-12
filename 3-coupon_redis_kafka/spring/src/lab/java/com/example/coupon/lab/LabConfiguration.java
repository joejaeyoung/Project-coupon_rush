package com.example.coupon.lab;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;

import java.util.Map;

@Configuration
@Profile("kafka-lab")
public class LabConfiguration {
    @Bean
    ConcurrentKafkaListenerContainerFactory<Object, Object> kafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ConsumerFactory<Object, Object> consumerFactory, LabProbe probe) {
        var factory = new ConcurrentKafkaListenerContainerFactory<Object, Object>();
        configurer.configure(factory, consumerFactory);
        if (factory.getContainerProperties().getAckMode() != ContainerProperties.AckMode.BATCH) {
            throw new IllegalStateException("The fault checkpoint requires BATCH ack mode");
        }
        factory.setRecordInterceptor(probe);
        return factory;
    }

    @Bean(destroyMethod = "close")
    Admin labAdmin(@Value("${spring.kafka.bootstrap-servers}") String bootstrap) {
        return Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 3000,
                AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 5000));
    }
}
