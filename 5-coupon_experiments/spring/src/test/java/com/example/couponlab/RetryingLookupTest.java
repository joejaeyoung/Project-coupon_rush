package com.example.couponlab;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class RetryingLookupTest {
    @Test void metadataOutageRetriesSamePolledRecordBeforeHandlingTheRestOfTheBatch() {
        TopicPartition partition=new TopicPartition("coupon",0);
        CommitBarrier barrier=new CommitBarrier();
        List<Long> handled=new ArrayList<>();
        AtomicInteger attempts=new AtomicInteger(), failures=new AtomicInteger(), backoffs=new AtomicInteger();
        for(long offset:List.of(10L,11L,12L)) {
            barrier.register(partition,offset);
            Optional<Long> loaded=RetryingLookup.load(()->{
                if(offset==10L && attempts.getAndIncrement()==0)throw new IllegalStateException("database unavailable before handler");
                return offset;
            },()->true,error->{failures.incrementAndGet();throw new IllegalStateException("metrics database also unavailable");},backoffs::incrementAndGet);
            loaded.ifPresent(value->{handled.add(value);barrier.complete(partition,value);});
        }
        assertThat(handled).containsExactly(10L,11L,12L);
        assertThat(attempts).hasValue(2);
        assertThat(failures).hasValue(1);
        assertThat(backoffs).hasValue(1);
        assertThat(barrier.committable().get(partition).offset()).isEqualTo(13);
    }

    @Test void shutdownLeavesUnreadPolledRecordUncommittedForTheNextConsumer() {
        TopicPartition partition=new TopicPartition("coupon",0);
        CommitBarrier barrier=new CommitBarrier();barrier.register(partition,10);
        AtomicBoolean running=new AtomicBoolean(true);
        Optional<Long> loaded=RetryingLookup.load(()->{throw new IllegalStateException("database unavailable");},
            running::get,error->{},()->running.set(false));
        assertThat(loaded).isEmpty();
        assertThat(barrier.committable()).isEmpty();
    }
}
