package com.example.couponlab;

import java.util.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

/** Completion may be out of order, but committed Kafka offsets must never cross unfinished work. */
final class CommitBarrier {
    private final Map<TopicPartition,TreeMap<Long,Boolean>> entries=new HashMap<>();
    void register(TopicPartition partition,long offset) { entries.computeIfAbsent(partition,p->new TreeMap<>()).putIfAbsent(offset,false); }
    void complete(TopicPartition partition,long offset) { entries.get(partition).put(offset,true); }
    void revoke(Collection<TopicPartition> partitions) { partitions.forEach(entries::remove); }
    Map<TopicPartition,OffsetAndMetadata> committable() {
        Map<TopicPartition,OffsetAndMetadata> offsets=new HashMap<>();
        entries.forEach((partition,records)->{
            Long last=null;
            for(var record:records.entrySet()) { if(!record.getValue())break;last=record.getKey(); }
            if(last!=null)offsets.put(partition,new OffsetAndMetadata(last+1));
        });
        return offsets;
    }
    void committed(Map<TopicPartition,OffsetAndMetadata> offsets) {
        offsets.forEach((partition,offset)->entries.get(partition).headMap(offset.offset()).clear());
    }
}
