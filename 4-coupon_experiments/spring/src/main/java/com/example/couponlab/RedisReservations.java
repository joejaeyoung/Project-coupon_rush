package com.example.couponlab;

import java.util.*;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;
import static com.example.couponlab.LabModel.*;

@Component
class RedisReservations {
    private final StringRedisTemplate redis;
    // Request identity is immutable in MySQL. The Redis request hash makes retrying a reservation idempotent.
    private static final DefaultRedisScript<Long> RESERVE = new DefaultRedisScript<>("""
        local prior = redis.call('HGET', KEYS[3], ARGV[1])
        if prior then return tonumber(prior) end
        if redis.call('EXISTS', KEYS[1]) == 0 then return -3 end
        if redis.call('SISMEMBER', KEYS[2], ARGV[2]) == 1 then
          redis.call('HSET', KEYS[3], ARGV[1], -1); return -1
        end
        if tonumber(redis.call('GET', KEYS[1])) <= 0 then
          redis.call('HSET', KEYS[3], ARGV[1], -2); return -2
        end
        redis.call('DECR', KEYS[1])
        redis.call('SADD', KEYS[2], ARGV[2])
        local seq = redis.call('INCR', KEYS[4])
        redis.call('HSET', KEYS[3], ARGV[1], seq)
        if ARGV[3] == 'STREAM' then redis.call('XADD', KEYS[5], '*', 'requestId', ARGV[1]) end
        return seq
        """,Long.class);
    RedisReservations(StringRedisTemplate redis) { this.redis=redis; }
    String prefix(Run run,long coupon) { return "lab:{"+run.id+":"+coupon+"}:"; }
    String stream(Run run,long coupon) { return prefix(run,coupon)+"events"; }
    void initialize(Run run) {
        if(run.strategy==Strategy.DB_OUTBOX) return;
        for(long c=1;c<=run.coupons;c++) {
            redis.opsForValue().set(prefix(run,c)+"stock",Integer.toString(run.stock));
            if(run.strategy==Strategy.REDIS_STREAM) {
                String key=stream(run,c);
                RecordId seed=redis.opsForStream().add(key,Map.of("requestId","__seed__"));
                redis.opsForStream().createGroup(key,ReadOffset.from("$"),"relay");
                redis.opsForStream().delete(key,seed);
            }
        }
    }
    long reserve(Run run,Map<String,Object> row,LabStore store) {
        long coupon=number(row,"coupon_id");String request=string(row,"request_id"), user=string(row,"user_id");
        String p=prefix(run,coupon);
        if(run.strategy==Strategy.SPLIT_REDIS) {
            // Deliberate baseline defect: a crash after SADD leaves a user marked without taking stock.
            Long added=redis.opsForSet().add(p+"users",user);
            if(added==null||added==0) return -1;
            if(store.consumeFault(run,request,"AFTER_ADD")) throw new LabException(503,"INJECTED_AFTER_ADD");
            Long stock=redis.opsForValue().decrement(p+"stock");
            if(stock==null) throw new IllegalStateException("Missing Redis DECR result");
            if(stock<0) { redis.opsForValue().increment(p+"stock");redis.opsForSet().remove(p+"users",user);return -2; }
            Long seq=redis.opsForValue().increment(p+"sequence");
            return seq==null?0:seq;
        }
        Long value=redis.execute(RESERVE,List.of(p+"stock",p+"users",p+"requests",p+"sequence",p+"events"),request,user,run.strategy==Strategy.REDIS_STREAM?"STREAM":"NONE");
        if(value==null) throw new IllegalStateException("Empty Lua result");
        return value;
    }
    Map<String,Object> snapshot(Run run,long coupon) {
        Map<String,Object> result=new LinkedHashMap<>();
        if(run.strategy==Strategy.DB_OUTBOX) { result.put("redisStock",null);result.put("redisUsers",null);return result; }
        try {
            String stock=redis.opsForValue().get(prefix(run,coupon)+"stock");
            result.put("redisStock",stock==null?null:Long.parseLong(stock));
            result.put("redisUsers",redis.opsForSet().size(prefix(run,coupon)+"users"));
        } catch(RuntimeException e) { result.put("redisStock",null);result.put("redisUsers",null);result.put("redisError",e.getClass().getSimpleName()); }
        return result;
    }
    List<MapRecord<String,Object,Object>> readStream(Run run,long coupon) {
        String key=stream(run,coupon);
        // A stable consumer name recovers this relay's pending list after JVM restart.
        var pending=redis.opsForStream().read(Consumer.from("relay","lab-relay"),StreamReadOptions.empty().count(50),StreamOffset.create(key,ReadOffset.from("0")));
        if(pending!=null&&!pending.isEmpty()) return pending;
        var fresh=redis.opsForStream().read(Consumer.from("relay","lab-relay"),StreamReadOptions.empty().count(50),StreamOffset.create(key,ReadOffset.lastConsumed()));
        return fresh==null?List.of():fresh;
    }
    void acknowledge(Run run,long coupon,RecordId id) { redis.opsForStream().acknowledge(stream(run,coupon),"relay",id); }
    Map<String,Object> streamBacklog(Run run) {
        Map<String,Object> result=new LinkedHashMap<>();long pel=0,lag=0;
        try {
            if(run.strategy==Strategy.REDIS_STREAM)for(long coupon=1;coupon<=run.coupons;coupon++) {
                String key=stream(run,coupon);
                var groups=redis.opsForStream().groups(key);
                var group=groups.stream().filter(g->"relay".equals(g.groupName())).findFirst().orElseThrow();
                pel+=group.pendingCount();
                Object groupLag=group.getRaw().get("lag");
                if(groupLag!=null)lag+=Long.parseLong(groupLag.toString());
                else {
                    // Redis returns unknown lag for some histories involving deleted seed
                    // entries; count actual entries beyond the delivery cursor in that case.
                    var undelivered=redis.opsForStream().range(key,Range.rightUnbounded(Range.Bound.exclusive(group.lastDeliveredId())));
                    if(undelivered!=null)lag+=undelivered.size();
                }
            }
            result.put("streamPel",pel);result.put("streamLag",lag);result.put("streamPending",pel+lag);
        } catch(RuntimeException error) {
            result.put("streamPel",null);result.put("streamLag",null);result.put("streamPending",null);
            result.put("streamError",KafkaTransport.rootMessage(error));
        }
        return result;
    }
}
