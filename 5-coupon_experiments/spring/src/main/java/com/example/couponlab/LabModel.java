package com.example.couponlab;

import java.util.*;

final class LabModel {
    enum Strategy { SPLIT_REDIS, LUA_REDIS, KAFKA_FIRST, DB_OUTBOX, REDIS_STREAM }
    enum Ordering { BLOCKING, RETRY_LATER }
    record NewRun(Strategy strategy, Integer stock, Integer coupons, Integer partitions, Integer workers, Ordering ordering) {}
    record NewRequest(String requestId, Long couponId, String userId, String fault, Long publishDelayMs, Long processingDelayMs) {}
    record Control(Boolean paused, Integer workers) {}
    record RequestRef(String requestId) {}
    static final class Run {
        final String id, topic;
        final Strategy strategy;
        final Ordering ordering;
        final int stock, coupons, partitions;
        volatile int workers;
        volatile boolean paused;
        Run(String id, String topic, Strategy strategy, Ordering ordering, int stock, int coupons, int partitions, int workers, boolean paused) {
            this.id=id; this.topic=topic; this.strategy=strategy; this.ordering=ordering;
            this.stock=stock; this.coupons=coupons; this.partitions=partitions; this.workers=workers; this.paused=paused;
        }
    }
    static final class LabException extends RuntimeException {
        final int code;
        LabException(int code, String reason) { super(reason); this.code=code; }
    }
    static String string(Map<String,Object> row, String key) { Object value=row.get(key); return value==null?null:value.toString(); }
    static long number(Map<String,Object> row, String key) { Object value=row.get(key); return value==null?0:((Number)value).longValue(); }
    static boolean flag(Map<String,Object> row, String key) { Object value=row.get(key); return value instanceof Boolean b?b:value instanceof Number n&&n.intValue()!=0; }
    static boolean terminal(String status) { return Set.of("ISSUED","REJECTED","CANCELLED").contains(status); }
    private LabModel() {}
}
