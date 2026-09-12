package com.example.coupon.lab;

import com.example.coupon.event.CouponIssueEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.listener.RecordInterceptor;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.io.FileOutputStream;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

@Component
@Profile("kafka-lab")
public class LabProbe implements RecordInterceptor<Object, Object> {
    public record Completion(long couponId, long userId, String topic, int partition,
                             long offset, long completedAtMs, String thread) { }

    private final ConcurrentLinkedQueue<Completion> completions = new ConcurrentLinkedQueue<>();
    private final AtomicLong completed = new AtomicLong();
    private final AtomicLong failures = new AtomicLong();
    private final AtomicLong faultUser = new AtomicLong(-1);
    private final JdbcTemplate jdbc;
    private final ObjectMapper json;
    private final Path marker;

    public LabProbe(JdbcTemplate jdbc, ObjectMapper json, @Value("${lab.fault-marker}") Path marker) {
        this.jdbc = jdbc;
        this.json = json;
        this.marker = marker;
    }

    @Override
    public ConsumerRecord<Object, Object> intercept(ConsumerRecord<Object, Object> record,
                                                   Consumer<Object, Object> consumer) {
        return record;
    }

    @Override
    public void success(ConsumerRecord<Object, Object> record, Consumer<Object, Object> consumer) {
        CouponIssueEvent event = (CouponIssueEvent) record.value();
        Completion completion = new Completion(event.getCouponId(), event.getUserId(), record.topic(),
                record.partition(), record.offset(), System.currentTimeMillis(), Thread.currentThread().getName());
        completions.add(completion);
        completed.incrementAndGet();
        if (faultUser.compareAndSet(event.getUserId(), -1)) {
            // The @Transactional listener has returned. BATCH has not committed this poll's offset.
            // Hold here so the external runner can inspect DB/offsets, then send a real SIGKILL.
            try {
                if (TransactionSynchronizationManager.isActualTransactionActive()) {
                    throw new IllegalStateException("Listener DB transaction is still active");
                }
                Integer rows = jdbc.queryForObject(
                        "SELECT COUNT(*) FROM issued_coupon WHERE coupon_id=? AND user_id=?", Integer.class,
                        event.getCouponId(), event.getUserId());
                TopicPartition tp = new TopicPartition(record.topic(), record.partition());
                var committed = consumer.committed(Set.of(tp)).get(tp);
                long committedOffset = committed == null ? -1 : committed.offset();
                if (rows == null || rows != 1 || committedOffset > record.offset()) {
                    throw new IllegalStateException("Fault boundary assertion failed");
                }
                byte[] bytes = json.writeValueAsBytes(Map.of("completion", completion,
                        "committedOffset", committedOffset, "dbRowsForEvent", rows,
                        "dbTransactionActive", false));
                try (FileOutputStream file = new FileOutputStream(marker.toFile())) {
                    file.write(bytes);
                    file.getFD().sync();
                }
                System.err.println("LAB_FAULT_READY partition=" + record.partition() + " offset=" + record.offset());
                new CountDownLatch(1).await();
            } catch (Exception ex) {
                failures.incrementAndGet();
                throw new IllegalStateException("Could not establish the fault boundary", ex);
            }
        }
    }

    @Override
    public void failure(ConsumerRecord<Object, Object> record, Exception ex, Consumer<Object, Object> consumer) {
        failures.incrementAndGet();
    }

    public void arm(long userId) { faultUser.set(userId); }
    public long count() { return completed.get(); }
    public long failures() { return failures.get(); }
    public List<Completion> completions() { return List.copyOf(completions); }
    public void reset() { completions.clear(); completed.set(0); failures.set(0); }
}
