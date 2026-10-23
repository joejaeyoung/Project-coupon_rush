package com.example.couponlab;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import static com.example.couponlab.LabModel.*;

@Service
class LabService implements ApplicationRunner {
    private final LabStore store;
    private final RedisReservations redis;
    private final KafkaTransport kafka;
    private final Map<String,Run> runs=new ConcurrentHashMap<>();
    private final Map<String,Object> dispatchLocks=new ConcurrentHashMap<>();
    private final Set<String> dispatching=ConcurrentHashMap.newKeySet();
    private volatile boolean ready;
    LabService(LabStore store,RedisReservations redis,KafkaTransport kafka) { this.store=store;this.redis=redis;this.kafka=kafka;kafka.setHandler(this::consume); }
    @Override public void run(ApplicationArguments args) {
        store.initialize();
        for(Run run:store.loadRuns()) { runs.put(run.id,run);kafka.restart(run); }
        ready=true;
    }
    Map<String,Object> health() { return Map.of("status",ready?"UP":"STARTING"); }
    Map<String,Object> create(NewRun input) {
        if(input.strategy()==null)throw new LabException(400,"STRATEGY_REQUIRED");
        int stock=or(input.stock(),100),coupons=or(input.coupons(),1),partitions=or(input.partitions(),3),workers=or(input.workers(),1);
        if(stock<0||stock>1000000||coupons<1||coupons>1000||partitions<1||partitions>100||workers<0||workers>32)throw new LabException(400,"INVALID_RUN_PARAMETERS");
        String id=UUID.randomUUID().toString();
        Run run=new Run(id,"coupon-lab-"+id,input.strategy(),input.ordering()==null?Ordering.BLOCKING:input.ordering(),stock,coupons,partitions,workers,false);
        kafka.createTopic(run);redis.initialize(run);store.create(run);runs.put(id,run);kafka.restart(run);
        return Map.of("runId",id,"topic",run.topic,"strategy",run.strategy,"couponIds",java.util.stream.LongStream.rangeClosed(1,coupons).boxed().toList());
    }
    private static int or(Integer value,int fallback) { return value==null?fallback:value; }
    Run run(String id) { Run run=runs.get(id);if(run==null)throw new LabException(404,"RUN_NOT_FOUND");return run; }
    Map<String,Object> submit(String runId,NewRequest request) {
        Run run=run(runId);validate(run,request);
        String dispatchKey=run.id+":"+request.requestId();
        synchronized(dispatchLocks.computeIfAbsent(dispatchKey,ignored->new Object())) {
            dispatching.add(dispatchKey);
            try {
                store.insert(run,request);
                Map<String,Object> row=store.request(run.id,request.requestId());
                if(terminal(string(row,"status")))return external(row);
                if(run.strategy==Strategy.KAFKA_FIRST) {
                    if(!flag(row,"published"))publish(run,row,true);
                    return external(store.request(run.id,request.requestId()));
                }
                boolean reserved=flag(row,"reserved");
                if(!reserved)reserved=run.strategy==Strategy.DB_OUTBOX?store.reserveOutbox(run,request.requestId()):reserveRedis(run,row);
                if(!reserved)return external(store.request(run.id,request.requestId()));
                inject(run,request.requestId(),"AFTER_RESERVE");
                if((run.strategy==Strategy.SPLIT_REDIS||run.strategy==Strategy.LUA_REDIS)&&!flag(row,"published"))publish(run,row,true);
                return external(store.request(run.id,request.requestId()));
            } finally { dispatching.remove(dispatchKey); }
        }
    }
    private void validate(Run run,NewRequest request) {
        if(request.requestId()==null||request.requestId().isBlank()||request.requestId().length()>128||request.userId()==null||request.userId().isBlank()||request.userId().length()>128||request.couponId()==null||request.couponId()<1||request.couponId()>run.coupons)throw new LabException(400,"INVALID_REQUEST");
        if(request.fault()!=null&&!Set.of("AFTER_ADD","AFTER_RESERVE","AFTER_SEND","AFTER_DB","READ_BEFORE_HANDLER").contains(request.fault()))throw new LabException(400,"UNKNOWN_FAULT");
        if("AFTER_ADD".equals(request.fault())&&run.strategy!=Strategy.SPLIT_REDIS)throw new LabException(400,"AFTER_ADD_ONLY_SPLIT");
        if(request.publishDelayMs()!=null&&(request.publishDelayMs()<0||request.publishDelayMs()>30000)||request.processingDelayMs()!=null&&(request.processingDelayMs()<0||request.processingDelayMs()>30000))throw new LabException(400,"INVALID_DELAY");
    }
    private boolean reserveRedis(Run run,Map<String,Object> row) {
        long result=redis.reserve(run,row,store);String id=string(row,"request_id");
        if(result==-3)throw new LabException(503,"REDIS_STATE_MISSING_NO_AUTOMATIC_RESET");
        if(result<0) { store.reject(run,id,result==-1?"DUPLICATE_USER":"SOLD_OUT");return false; }
        store.reserved(run,id,result);return true;
    }
    void publish(Run run,Map<String,Object> row,boolean faultInjection) {
        long delay=number(row,"publish_delay_ms");
        if(delay>0)try { Thread.sleep(delay); } catch(InterruptedException e) { Thread.currentThread().interrupt();throw new LabException(503,"INTERRUPTED"); }
        RecordMetadata metadata=kafka.send(run,row);
        if(faultInjection)inject(run,string(row,"request_id"),"AFTER_SEND");
        store.published(run,string(row,"request_id"),metadata.partition(),metadata.offset());
    }
    private void inject(Run run,String request,String fault) { if(store.consumeFault(run,request,fault))throw new LabException(503,"INJECTED_"+fault); }
    private void consume(Run run,ConsumerRecord<String,String> record) {
        String id=record.value();Map<String,Object> row=store.request(run.id,id);
        if(run.strategy==Strategy.KAFKA_FIRST&&!terminal(string(row,"status"))&&!flag(row,"reserved")) {
            if(!reserveRedis(run,row))return;
            inject(run,id,"AFTER_RESERVE");
        }
        store.finalizeIssue(run,id,record.partition(),record.offset());
        inject(run,id,"AFTER_DB");
    }
    @Scheduled(fixedDelay=100)
    void relay() {
        if(!ready)return;
        for(Run run:runs.values()) {
            if(run.paused)continue;
            try {
                if(run.strategy==Strategy.DB_OUTBOX) {
                    for(Map<String,Object> item:store.jdbc.queryForList("SELECT request_id FROM lab_outbox WHERE run_id=? AND sent=FALSE ORDER BY id LIMIT 50",run.id)) {
                        String id=string(item,"request_id");Map<String,Object> row=store.request(run.id,id);
                        if(dispatching.contains(run.id+":"+id))continue;
                        if(!"CANCELLED".equals(string(row,"status")))publish(run,row,true);
                        store.jdbc.update("UPDATE lab_outbox SET sent=TRUE WHERE run_id=? AND request_id=?",run.id,id);
                    }
                } else if(run.strategy==Strategy.REDIS_STREAM) {
                    for(long coupon=1;coupon<=run.coupons;coupon++)for(var record:redis.readStream(run,coupon)) {
                        Object requestId=record.getValue().get("requestId");
                        if(requestId!=null&&dispatching.contains(run.id+":"+requestId))continue;
                        if(requestId!=null&&!"__seed__".equals(requestId.toString()))publish(run,store.request(run.id,requestId.toString()),true);
                        redis.acknowledge(run,coupon,record.getId());
                    }
                }
            } catch(RuntimeException e) { try { store.error(run,"relay_errors"); } catch(RuntimeException ignored) {} }
        }
    }
    Map<String,Object> control(String id,Control control) {
        Run run=run(id);
        if(control.workers()!=null&&(control.workers()<0||control.workers()>32))throw new LabException(400,"INVALID_WORKERS");
        if(control.paused()!=null)run.paused=control.paused();
        boolean restart=control.workers()!=null;
        if(control.workers()!=null)run.workers=control.workers();
        store.jdbc.update("UPDATE lab_runs SET paused=?,workers=? WHERE run_id=?",run.paused,run.workers,id);
        if(restart)kafka.restart(run);
        return Map.of("runId",id,"paused",run.paused,"workers",run.workers);
    }
    Map<String,Object> replay(String id,String request) { Run run=run(id);publish(run,store.request(id,request),false);return Map.of("requestId",request,"status","PENDING"); }
    Map<String,Object> cancel(String id,String request) { return external(store.cancel(run(id),request)); }
    Map<String,Object> request(String id,String request) { run(id);return external(store.request(id,request)); }
    Map<String,Object> snapshot(String id) {
        Run run=run(id);List<Map<String,Object>> rows=store.requests(id);
        Map<String,Object> result=new LinkedHashMap<>();result.put("runId",id);result.put("strategy",run.strategy);result.put("topic",run.topic);
        result.put("ordering",run.ordering);result.put("paused",run.paused);result.put("workers",run.workers);result.put("partitions",run.partitions);
        result.put("received",rows.size());result.put("accepted",rows.stream().filter(r->flag(r,"accepted")).count());
        result.put("reserved",rows.stream().filter(r->flag(r,"reserved")).count());
        for(String status:List.of("ISSUED","REJECTED","CANCELLED"))result.put(status.toLowerCase(),rows.stream().filter(r->status.equals(string(r,"status"))).count());
        result.put("pending",rows.stream().filter(r->!terminal(string(r,"status"))).count());
        result.put("activeDbReservations",rows.stream().filter(r->flag(r,"db_reserved")).count());
        result.put("ledgerIssued",store.jdbc.queryForObject("SELECT COUNT(*) FROM lab_issued WHERE run_id=?",Long.class,id));
        result.put("ledgerDuplicateUsers",store.jdbc.queryForObject("SELECT COUNT(*) FROM (SELECT coupon_id,user_id FROM lab_issued WHERE run_id=? GROUP BY coupon_id,user_id HAVING COUNT(*)>1) duplicates",Long.class,id));
        result.put("outboxPending",store.jdbc.queryForObject("SELECT COUNT(*) FROM lab_outbox WHERE run_id=? AND sent=FALSE",Long.class,id));
        result.put("unpublishedStreamReservations",run.strategy==Strategy.REDIS_STREAM?rows.stream().filter(r->flag(r,"reserved")&&!flag(r,"published")).count():0L);
        result.putAll(redis.streamBacklog(run));
        Map<String,Object> counters=store.jdbc.queryForMap("SELECT consumer_errors,replays,relay_errors FROM lab_runs WHERE run_id=?",id);
        result.put("consumerErrors",counters.get("consumer_errors"));result.put("replays",counters.get("replays"));result.put("relayErrors",counters.get("relay_errors"));
        List<Map<String,Object>> coupons=new ArrayList<>();
        for(Map<String,Object> c:store.jdbc.queryForList("SELECT * FROM lab_coupons WHERE run_id=? ORDER BY coupon_id",id)) {
            long coupon=number(c,"coupon_id");Map<String,Object> item=new LinkedHashMap<>();item.put("couponId",coupon);item.put("total",c.get("total"));
            item.put("dbRemaining",c.get("remaining"));item.put("issued",rows.stream().filter(r->number(r,"coupon_id")==coupon&&"ISSUED".equals(string(r,"status"))).count());
            item.put("ledgerIssued",store.jdbc.queryForObject("SELECT COUNT(*) FROM lab_issued WHERE run_id=? AND coupon_id=?",Long.class,id,coupon));
            item.putAll(redis.snapshot(run,coupon));coupons.add(item);
        }
        result.put("perCoupon",coupons);result.put("requests",rows.stream().map(this::external).toList());return result;
    }
    Map<String,Object> external(Map<String,Object> row) {
        Map<String,Object> value=new LinkedHashMap<>();
        String[][] fields={{"requestId","request_id"},{"couponId","coupon_id"},{"userId","user_id"},{"status","status"},{"reason","reason"},
            {"partition","kafka_partition"},{"offset","kafka_offset"},{"reservationSeq","reservation_seq"},{"completionSeq","completion_seq"},
            {"receivedAt","received_at"},{"completedAt","completed_at"},{"reserved","reserved"},{"published","published"},{"fault","fault"},
            {"faultConsumed","fault_consumed"},{"processingDelayMs","processing_delay_ms"},{"dbReserved","db_reserved"}};
        for(String[] f:fields)value.put(f[0],row.get(f[1]));return value;
    }
}
