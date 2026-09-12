package com.example.coupon.lab;

import com.example.coupon.domain.coupon.Coupon;
import com.example.coupon.domain.coupon.Status;
import com.example.coupon.repository.CouponRepository;
import com.example.coupon.service.CouponRedisService;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.utils.AppInfoParser;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@RestController
@Profile("kafka-lab")
@RequestMapping("/lab")
public class LabController {
    private final CouponRepository coupons;
    private final CouponRedisService redis;
    private final StringRedisTemplate strings;
    private final NamedParameterJdbcTemplate jdbc;
    private final KafkaListenerEndpointRegistry registry;
    private final LabProbe probe;
    private final Admin admin;
    private final String topic;
    private final String group;
    private final int partitions;
    private final ConsumerFactory<Object, Object> consumerFactory;

    public LabController(CouponRepository coupons, CouponRedisService redis, StringRedisTemplate strings,
                         NamedParameterJdbcTemplate jdbc, KafkaListenerEndpointRegistry registry,
                         LabProbe probe, Admin admin, @Value("${coupon.kafka.topic}") String topic,
                         @Value("${spring.kafka.consumer.group-id}") String group,
                         @Value("${coupon.kafka.partitions}") int partitions,
                         ConsumerFactory<Object, Object> consumerFactory) {
        this.coupons = coupons; this.redis = redis; this.strings = strings; this.jdbc = jdbc;
        this.registry = registry; this.probe = probe; this.admin = admin;
        this.topic = topic; this.group = group; this.partitions = partitions;
        this.consumerFactory = consumerFactory;
    }

    public record Seed(int couponCount, int stockPerCoupon) { }

    @PostMapping("/seed")
    public List<Long> seed(@RequestBody Seed input) {
        if (input.couponCount() < 1 || input.couponCount() > 1000 || input.stockPerCoupon() < 1) {
            throw new IllegalArgumentException("Invalid experiment stock");
        }
        List<Long> ids = new ArrayList<>();
        for (int i = 0; i < input.couponCount(); i++) {
            Coupon c = new Coupon();
            c.setName("kafka-lab"); c.setStatus(Status.ACTIVE);
            c.setTotalQuantity(input.stockPerCoupon()); c.setIssuedQuantity(0); c.setValidDays(30);
            c.setStartDate(LocalDateTime.now().minusDays(1)); c.setEndDate(LocalDateTime.now().plusDays(1));
            c = coupons.save(c);
            redis.initStock(c.getId(), c.getTotalQuantity(), c.getStartDate(), c.getEndDate());
            ids.add(c.getId());
        }
        return ids;
    }

    @GetMapping("/status")
    public Map<String, Object> status() {
        var container = registry.getListenerContainer("coupon-issuer");
        var assigned = container.getAssignmentsByClientId();
        Map<String, List<Integer>> assignments = new LinkedHashMap<>();
        if (assigned != null) assigned.forEach((client, tps) -> assignments.put(client,
                tps == null ? List.of() : tps.stream().map(TopicPartition::partition).sorted().toList()));
        return Map.of("running", container.isRunning(), "paused", container.isContainerPaused(),
                "assignments", assignments, "completed", probe.count(), "failures", probe.failures());
    }

    @GetMapping("/config")
    public Map<String, Object> config() {
        var resolved = new ConsumerConfig(consumerFactory.getConfigurationProperties()).values();
        Map<String, Object> values = new LinkedHashMap<>();
        for (String key : List.of("enable.auto.commit", "max.poll.records", "max.poll.interval.ms",
                "session.timeout.ms", "heartbeat.interval.ms", "auto.offset.reset")) {
            values.put(key, resolved.get(key));
        }
        return Map.of("consumer", values, "kafkaClientVersion", AppInfoParser.getVersion(),
                "javaVersion", System.getProperty("java.version"),
                "ackMode", registry.getListenerContainer("coupon-issuer").getContainerProperties().getAckMode().name(),
                "partitions", partitions, "topic", topic, "group", group);
    }

    @GetMapping("/lag")
    public Map<String, Object> lag() throws Exception {
        Map<TopicPartition, OffsetSpec> request = new LinkedHashMap<>();
        for (int p = 0; p < partitions; p++) request.put(new TopicPartition(topic, p), OffsetSpec.latest());
        var committed = admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(5, TimeUnit.SECONDS);
        var ends = admin.listOffsets(request).all().get(5, TimeUnit.SECONDS);
        List<Map<String, Long>> offsets = new ArrayList<>();
        long totalLag = 0;
        for (var entry : ends.entrySet()) {
            long end = entry.getValue().offset();
            var offset = committed.get(entry.getKey());
            long current = offset == null ? 0 : offset.offset();
            totalLag += end - current;
            offsets.add(Map.of("partition", (long) entry.getKey().partition(), "end", end, "committed", current));
        }
        return Map.of("atMs", System.currentTimeMillis(), "lag", totalLag,
                "offsets", offsets, "completed", probe.count(), "failures", probe.failures());
    }

    @GetMapping("/completions")
    public List<LabProbe.Completion> completions() { return probe.completions(); }

    @PostMapping("/reset-metrics")
    public void resetMetrics() { probe.reset(); }

    @PostMapping("/pause")
    public void pause() { registry.getListenerContainer("coupon-issuer").pause(); }

    @PostMapping("/resume")
    public void resume() { registry.getListenerContainer("coupon-issuer").resume(); }

    @PostMapping("/arm")
    public void arm(@RequestParam long userId) { probe.arm(userId); }

    @GetMapping("/state")
    public Map<String, Object> state(@RequestParam List<Long> ids) {
        Map<String, Object> params = Map.of("ids", ids);
        var db = jdbc.queryForList("SELECT c.id, c.total_quantity, c.issued_quantity, COUNT(i.id) AS db_rows " +
                "FROM coupon c LEFT JOIN issued_coupon i ON i.coupon_id=c.id " +
                "WHERE c.id IN (:ids) GROUP BY c.id ORDER BY c.id", params);
        var duplicates = jdbc.queryForList("SELECT coupon_id, user_id, COUNT(*) AS copies FROM issued_coupon " +
                "WHERE coupon_id IN (:ids) GROUP BY coupon_id, user_id HAVING COUNT(*) > 1", params);
        List<Map<String, Object>> cache = new ArrayList<>();
        for (long id : ids) cache.add(Map.of("id", id,
                "stock", strings.opsForValue().get("coupon:" + id + ":stock"),
                "users", strings.opsForSet().size("coupon:" + id + ":users")));
        return Map.of("coupons", db, "duplicateGroups", duplicates, "redis", cache);
    }
}
