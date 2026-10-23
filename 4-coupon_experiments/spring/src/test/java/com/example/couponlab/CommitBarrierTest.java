package com.example.couponlab;

import java.util.List;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class CommitBarrierTest {
    final TopicPartition a=new TopicPartition("coupon",0),b=new TopicPartition("coupon",1);
    @Test void newerCompletionCannotAcknowledgeAnUnfinishedOlderRequest() {
        CommitBarrier barrier=new CommitBarrier();
        barrier.register(a,40);barrier.register(a,41);barrier.register(a,42);
        barrier.complete(a,41);barrier.complete(a,42);
        assertThat(barrier.committable()).isEmpty();
        barrier.complete(a,40);
        assertThat(barrier.committable().get(a).offset()).isEqualTo(43);
        barrier.committed(barrier.committable());
        assertThat(barrier.committable()).isEmpty();
    }
    @Test void oneSlowCouponPartitionDoesNotBlockAnotherPartitionCommit() {
        CommitBarrier barrier=new CommitBarrier();
        barrier.register(a,10);barrier.register(b,90);barrier.complete(b,90);
        assertThat(barrier.committable()).containsOnlyKeys(b);
        assertThat(barrier.committable().get(b).offset()).isEqualTo(91);
    }
    @Test void failedCommitRemainsRetryableAndRebalanceDiscardsPreviousOwnership() {
        CommitBarrier barrier=new CommitBarrier();
        barrier.register(a,4);barrier.complete(a,4);
        var failedAttempt=barrier.committable();
        assertThat(barrier.committable()).isEqualTo(failedAttempt);
        barrier.revoke(List.of(a));
        assertThat(barrier.committable()).isEmpty();
        barrier.register(a,4);
        assertThat(barrier.committable()).isEmpty();
    }
}
