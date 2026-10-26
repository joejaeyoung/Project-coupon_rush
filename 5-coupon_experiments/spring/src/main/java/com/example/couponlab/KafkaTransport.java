package com.example.couponlab;

import jakarta.annotation.PreDestroy;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.BiConsumer;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import static com.example.couponlab.LabModel.*;

@Component
class KafkaTransport {
    private final String bootstrap;
    private final LabStore store;
    private final KafkaProducer<String,String> producer;
    private final Admin admin;
    private final Map<String,List<Worker>> workers=new ConcurrentHashMap<>();
    private BiConsumer<Run,ConsumerRecord<String,String>> handler;

    KafkaTransport(@Value("${lab.kafka.bootstrap}") String bootstrap,LabStore store) {
        this.bootstrap=bootstrap;this.store=store;
        Properties p=new Properties();p.put("bootstrap.servers",bootstrap);
        p.put("key.serializer",StringSerializer.class.getName());p.put("value.serializer",StringSerializer.class.getName());
        p.put("acks","all");p.put("enable.idempotence","true");p.put("delivery.timeout.ms","10000");
        p.put("request.timeout.ms","3000");p.put("max.block.ms","10000");p.put("linger.ms","0");
        producer=new KafkaProducer<>(p);
        admin=Admin.create(Map.of("bootstrap.servers",bootstrap,"request.timeout.ms","5000","default.api.timeout.ms","15000"));
    }
    void setHandler(BiConsumer<Run,ConsumerRecord<String,String>> handler) { this.handler=handler; }
    void createTopic(Run run) {
        try { admin.createTopics(List.of(new NewTopic(run.topic,run.partitions,(short)3).configs(Map.of("min.insync.replicas","2")))).all().get(20,TimeUnit.SECONDS); }
        catch(Exception e) { throw new LabException(503,"TOPIC_CREATE_FAILED: "+rootMessage(e)); }
    }
    RecordMetadata send(Run run,Map<String,Object> row) {
        try { return producer.send(new ProducerRecord<>(run.topic,string(row,"coupon_id"),string(row,"request_id"))).get(12,TimeUnit.SECONDS); }
        catch(Exception e) { throw new LabException(503,"KAFKA_ACK_FAILED: "+rootMessage(e)); }
    }
    static String rootMessage(Throwable e) { while(e.getCause()!=null)e=e.getCause(); return e.getClass().getSimpleName()+": "+e.getMessage(); }
    synchronized void restart(Run run) {
        // Retain ownership on join timeout so the next control call must stop these same workers before replacement.
        List<Worker> old=workers.get(run.id);
        if(old!=null) { old.forEach(Worker::stop);old.forEach(Worker::join); }
        List<Worker> active=new ArrayList<>();
        for(int n=0;n<run.workers;n++) { Worker worker=new Worker(run,n);active.add(worker);worker.thread.start(); }
        workers.put(run.id,active);
    }
    @PreDestroy void close() {
        workers.values().forEach(ws->ws.forEach(Worker::stop));workers.values().forEach(ws->ws.forEach(Worker::join));
        producer.close(Duration.ofSeconds(5));admin.close(Duration.ofSeconds(5));
    }
    private final class Worker implements Runnable {
        final Run run;
        final Thread thread;
        volatile boolean running=true;
        volatile KafkaConsumer<String,String> consumer;
        final CommitBarrier barrier=new CommitBarrier();
        final List<Deferred> deferred=new ArrayList<>();
        Worker(Run run,int index) { this.run=run;thread=new Thread(this,"coupon-"+run.id+"-"+index);thread.setDaemon(true); }
        void stop() { running=false;KafkaConsumer<String,String> c=consumer;if(c!=null)c.wakeup();thread.interrupt(); }
        void join() {
            try { thread.join(10000); } catch(InterruptedException e) { Thread.currentThread().interrupt();throw new LabException(503,"CONSUMER_STOP_INTERRUPTED"); }
            if(thread.isAlive())throw new LabException(503,"CONSUMER_STILL_STOPPING_RETRY_CONTROL");
        }
        @Override public void run() {
            Properties p=new Properties();p.put("bootstrap.servers",bootstrap);p.put("group.id","lab-"+run.id);
            p.put("key.deserializer",StringDeserializer.class.getName());p.put("value.deserializer",StringDeserializer.class.getName());
            p.put("enable.auto.commit","false");p.put("auto.offset.reset","earliest");p.put("max.poll.records","50");
            p.put("session.timeout.ms","10000");p.put("heartbeat.interval.ms","2000");p.put("max.poll.interval.ms","300000");
            try(KafkaConsumer<String,String> c=new KafkaConsumer<>(p)) {
                consumer=c;
                c.subscribe(List.of(run.topic),new ConsumerRebalanceListener() {
                    @Override public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                        commitSafe();
                        barrier.revoke(partitions);
                        deferred.removeIf(d->partitions.contains(partition(d.record)));
                    }
                    @Override public void onPartitionsAssigned(Collection<TopicPartition> partitions) {}
                });
                while(running) {
                    try {
                        if(run.paused)c.pause(c.assignment());else c.resume(c.assignment());
                        ConsumerRecords<String,String> records=c.poll(Duration.ofMillis(100));
                        if(run.paused) {
                            for(TopicPartition tp:records.partitions())c.seek(tp,records.records(tp).get(0).offset());
                            continue;
                        }
                        for(ConsumerRecord<String,String> record:records) {
                            if(!running)break;
                            barrier.register(partition(record),record.offset());
                            // poll() already advanced the position. A failed metadata lookup must
                            // retry this record, otherwise the rest of this batch would be skipped.
                            Optional<Map<String,Object>> loaded=RetryingLookup.load(
                                ()->store.consumerRequest(run,record.value()),()->running,
                                ignored->recordError(),()->sleep(200));
                            if(loaded.isEmpty())break;
                            Map<String,Object> request=loaded.get();
                            long delay=terminal(string(request,"status"))?0:number(request,"processing_delay_ms");
                            if(run.ordering==Ordering.RETRY_LATER&&delay>0) deferred.add(new Deferred(record,System.currentTimeMillis()+delay));
                            else processBlocking(record,delay);
                        }
                        // Retry-later changes completion order, while holding commits behind the earliest unfinished record.
                        Iterator<Deferred> iterator=deferred.iterator();
                        while(iterator.hasNext()) {
                            Deferred d=iterator.next();
                            if(System.currentTimeMillis()<d.due)continue;
                            try { handler.accept(run,d.record);markFinished(d.record);iterator.remove(); }
                            catch(RuntimeException e) { recordError();d.due=System.currentTimeMillis()+200; }
                        }
                        commitSafe();
                    } catch(WakeupException e) { if(running)throw e; }
                    catch(RuntimeException e) { if(running) { recordError();sleep(200); } }
                }
                commitSafe();
            } finally { consumer=null; }
        }
        void processBlocking(ConsumerRecord<String,String> record,long delay) {
            if(delay>0) sleep(delay);
            while(running) {
                try { handler.accept(run,record);markFinished(record);return; }
                catch(RuntimeException e) {
                    recordError();
                    if(run.ordering==Ordering.RETRY_LATER) { deferred.add(new Deferred(record,System.currentTimeMillis()+200));return; }
                    sleep(200);
                }
            }
        }
        void markFinished(ConsumerRecord<String,String> r) { barrier.complete(partition(r),r.offset()); }
        void commitSafe() {
            if(consumer==null)return;
            Map<TopicPartition,OffsetAndMetadata> offsets=barrier.committable();
            if(offsets.isEmpty())return;
            try {
                consumer.commitSync(offsets,Duration.ofSeconds(3));
                barrier.committed(offsets);
            } catch(RuntimeException e) { if(running)recordError(); }
        }
        void recordError() { try { store.error(run,"consumer_errors"); } catch(RuntimeException ignored) {} }
        void sleep(long ms) { try { Thread.sleep(ms); } catch(InterruptedException e) { if(running)Thread.currentThread().interrupt(); } }
        TopicPartition partition(ConsumerRecord<String,String> r) { return new TopicPartition(r.topic(),r.partition()); }
    }
    private static final class Deferred {
        final ConsumerRecord<String,String> record;long due;
        Deferred(ConsumerRecord<String,String> record,long due) { this.record=record;this.due=due; }
    }
}
