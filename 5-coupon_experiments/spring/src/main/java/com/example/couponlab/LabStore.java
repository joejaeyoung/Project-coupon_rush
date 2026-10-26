package com.example.couponlab;

import java.util.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;
import static com.example.couponlab.LabModel.*;

/** MySQL is the final issuance authority. Every capacity check and final insert share one transaction. */
@Component
class LabStore {
    final JdbcTemplate jdbc;
    final TransactionTemplate tx;
    LabStore(JdbcTemplate jdbc, TransactionTemplate tx) { this.jdbc=jdbc; this.tx=tx; }

    void initialize() {
        jdbc.execute("""
            CREATE TABLE IF NOT EXISTS lab_runs (
              run_id VARCHAR(40) PRIMARY KEY, topic VARCHAR(100) NOT NULL, strategy VARCHAR(30) NOT NULL,
              ordering_mode VARCHAR(20) NOT NULL, stock INT NOT NULL, coupons INT NOT NULL, partitions_count INT NOT NULL,
              workers INT NOT NULL, paused BOOLEAN NOT NULL DEFAULT FALSE, reservation_seq BIGINT NOT NULL DEFAULT 0,
              completion_seq BIGINT NOT NULL DEFAULT 0, consumer_errors BIGINT NOT NULL DEFAULT 0,
              replays BIGINT NOT NULL DEFAULT 0, relay_errors BIGINT NOT NULL DEFAULT 0)
            """);
        jdbc.execute("""
            CREATE TABLE IF NOT EXISTS lab_coupons (
              run_id VARCHAR(40) NOT NULL, coupon_id BIGINT NOT NULL, total INT NOT NULL, remaining INT NOT NULL,
              reservation_seq BIGINT NOT NULL DEFAULT 0, completion_seq BIGINT NOT NULL DEFAULT 0,
              PRIMARY KEY(run_id,coupon_id))
            """);
        jdbc.execute("""
            CREATE TABLE IF NOT EXISTS lab_requests (
              run_id VARCHAR(40) NOT NULL, request_id VARCHAR(128) NOT NULL, coupon_id BIGINT NOT NULL,
              user_id VARCHAR(128) NOT NULL, status VARCHAR(20) NOT NULL DEFAULT 'RECEIVED', reason VARCHAR(100),
              accepted BOOLEAN NOT NULL DEFAULT FALSE, reserved BOOLEAN NOT NULL DEFAULT FALSE,
              db_reserved BOOLEAN NOT NULL DEFAULT FALSE, published BOOLEAN NOT NULL DEFAULT FALSE,
              fault VARCHAR(30), fault_consumed BOOLEAN NOT NULL DEFAULT FALSE,
              publish_delay_ms BIGINT NOT NULL DEFAULT 0, processing_delay_ms BIGINT NOT NULL DEFAULT 0,
              kafka_partition INT, kafka_offset BIGINT, reservation_seq BIGINT, completion_seq BIGINT,
              received_at BIGINT NOT NULL, completed_at BIGINT,
              PRIMARY KEY(run_id,request_id), INDEX lab_status(run_id,status))
            """);
        jdbc.execute("""
            CREATE TABLE IF NOT EXISTS lab_issued (
              run_id VARCHAR(40) NOT NULL, coupon_id BIGINT NOT NULL, user_id VARCHAR(128) NOT NULL,
              request_id VARCHAR(128) NOT NULL, PRIMARY KEY(run_id,coupon_id,user_id), UNIQUE(run_id,request_id))
            """);
        jdbc.execute("""
            CREATE TABLE IF NOT EXISTS lab_outbox (
              id BIGINT AUTO_INCREMENT PRIMARY KEY, run_id VARCHAR(40) NOT NULL, request_id VARCHAR(128) NOT NULL,
              sent BOOLEAN NOT NULL DEFAULT FALSE, UNIQUE(run_id,request_id), INDEX lab_unsent(run_id,sent))
            """);
        // Opaque request/user IDs must have the same case and trailing-space identity
        // semantics in MySQL as Redis. Migrate only this lab's tables, preserving rows.
        for(String table:List.of("lab_runs","lab_coupons","lab_requests","lab_issued","lab_outbox")) {
            String collation=jdbc.queryForObject("SELECT TABLE_COLLATION FROM information_schema.TABLES WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=?",String.class,table);
            if(!"utf8mb4_0900_bin".equals(collation))jdbc.execute("ALTER TABLE "+table+" CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin");
        }
    }
    List<Run> loadRuns() {
        return jdbc.query("SELECT * FROM lab_runs", (rs,n)->new Run(rs.getString("run_id"),rs.getString("topic"),
            Strategy.valueOf(rs.getString("strategy")),Ordering.valueOf(rs.getString("ordering_mode")),rs.getInt("stock"),
            rs.getInt("coupons"),rs.getInt("partitions_count"),rs.getInt("workers"),rs.getBoolean("paused")));
    }
    void create(Run run) {
        tx.executeWithoutResult(s->{
            jdbc.update("INSERT INTO lab_runs(run_id,topic,strategy,ordering_mode,stock,coupons,partitions_count,workers,paused) VALUES(?,?,?,?,?,?,?,?,?)",
                run.id,run.topic,run.strategy.name(),run.ordering.name(),run.stock,run.coupons,run.partitions,run.workers,run.paused);
            for(long c=1;c<=run.coupons;c++) jdbc.update("INSERT INTO lab_coupons(run_id,coupon_id,total,remaining) VALUES(?,?,?,?)",run.id,c,run.stock,run.stock);
        });
    }
    boolean insert(Run run, NewRequest request) {
        int inserted=jdbc.update("""
            INSERT IGNORE INTO lab_requests(run_id,request_id,coupon_id,user_id,fault,publish_delay_ms,processing_delay_ms,received_at)
            VALUES(?,?,?,?,?,?,?,?)
            """,run.id,request.requestId(),request.couponId(),request.userId(),request.fault(),
            request.publishDelayMs()==null?0:request.publishDelayMs(),request.processingDelayMs()==null?0:request.processingDelayMs(),System.currentTimeMillis());
        Map<String,Object> row=request(run.id,request.requestId());
        if(number(row,"coupon_id")!=request.couponId() || !Objects.equals(string(row,"user_id"),request.userId()))
            throw new LabException(409,"REQUEST_ID_PAYLOAD_CONFLICT");
        return inserted==1;
    }
    Map<String,Object> request(String run, String request) {
        List<Map<String,Object>> rows=jdbc.queryForList("SELECT * FROM lab_requests WHERE run_id=? AND request_id=?",run,request);
        if(rows.isEmpty()) throw new LabException(404,"REQUEST_NOT_FOUND");
        return rows.get(0);
    }
    Map<String,Object> consumerRequest(Run run,String request) {
        Map<String,Object> row=request(run.id,request);
        // Deterministic failure at the pre-handler read seam; this is a fault hook,
        // while the same retry also covers actual JDBC read failures.
        if("READ_BEFORE_HANDLER".equals(string(row,"fault"))&&!flag(row,"fault_consumed")&&consumeFault(run,request,"READ_BEFORE_HANDLER"))
            throw new LabException(503,"INJECTED_READ_BEFORE_HANDLER");
        return row;
    }
    List<Map<String,Object>> requests(String run) { return jdbc.queryForList("SELECT * FROM lab_requests WHERE run_id=? ORDER BY received_at,request_id",run); }
    void reject(Run run,String id,String reason) { jdbc.update("UPDATE lab_requests SET status='REJECTED',reason=?,completed_at=? WHERE run_id=? AND request_id=? AND status NOT IN ('ISSUED','CANCELLED')",reason,System.currentTimeMillis(),run.id,id); }
    void reserved(Run run,String id,long seq) { jdbc.update("UPDATE lab_requests SET reserved=TRUE,accepted=TRUE,status='RESERVED',reservation_seq=? WHERE run_id=? AND request_id=? AND status NOT IN ('ISSUED','REJECTED','CANCELLED')",seq,run.id,id); }
    void published(Run run,String id,int partition,long offset) {
        jdbc.update("UPDATE lab_requests SET accepted=TRUE,published=TRUE,kafka_partition=IF(status='ISSUED',kafka_partition,?),kafka_offset=IF(status='ISSUED',kafka_offset,?),status=IF(status IN ('RECEIVED','RESERVED'),'PENDING',status) WHERE run_id=? AND request_id=?",partition,offset,run.id,id);
    }
    boolean consumeFault(Run run,String id,String fault) {
        return jdbc.update("UPDATE lab_requests SET fault_consumed=TRUE WHERE run_id=? AND request_id=? AND fault=? AND fault_consumed=FALSE",run.id,id,fault)==1;
    }
    void error(Run run,String counter) {
        if(!Set.of("consumer_errors","relay_errors","replays").contains(counter)) throw new IllegalArgumentException();
        jdbc.update("UPDATE lab_runs SET "+counter+"="+counter+"+1 WHERE run_id=?",run.id);
    }

    boolean reserveOutbox(Run run,String id) {
        return Boolean.TRUE.equals(tx.execute(s->{
            Map<String,Object> row=jdbc.queryForMap("SELECT * FROM lab_requests WHERE run_id=? AND request_id=? FOR UPDATE",run.id,id);
            if(terminal(string(row,"status"))) return false;
            if(flag(row,"db_reserved")) return true;
            long coupon=number(row,"coupon_id");
            // Coupon row serializes different request IDs for the same user as well as stock.
            jdbc.queryForMap("SELECT remaining FROM lab_coupons WHERE run_id=? AND coupon_id=? FOR UPDATE",run.id,coupon);
            Long duplicates=jdbc.queryForObject("SELECT COUNT(*) FROM lab_requests WHERE run_id=? AND coupon_id=? AND user_id=? AND request_id<>? AND (db_reserved=TRUE OR status='ISSUED')",Long.class,run.id,coupon,row.get("user_id"),id);
            if(duplicates!=null&&duplicates>0) { reject(run,id,"DUPLICATE_USER");return false; }
            int updated=jdbc.update("UPDATE lab_coupons SET remaining=remaining-1 WHERE run_id=? AND coupon_id=? AND remaining>0",run.id,coupon);
            if(updated==0) { reject(run,id,"SOLD_OUT");return false; }
            jdbc.update("UPDATE lab_coupons SET reservation_seq=reservation_seq+1 WHERE run_id=? AND coupon_id=?",run.id,coupon);
            Long seq=jdbc.queryForObject("SELECT reservation_seq FROM lab_coupons WHERE run_id=? AND coupon_id=?",Long.class,run.id,coupon);
            jdbc.update("UPDATE lab_requests SET db_reserved=TRUE,reserved=TRUE,accepted=TRUE,status='RESERVED',reservation_seq=? WHERE run_id=? AND request_id=?",seq,run.id,id);
            jdbc.update("INSERT INTO lab_outbox(run_id,request_id) VALUES(?,?)",run.id,id);
            return true;
        }));
    }

    void finalizeIssue(Run run,String id,int partition,long offset) {
        tx.executeWithoutResult(s->{
            Map<String,Object> row=jdbc.queryForMap("SELECT * FROM lab_requests WHERE run_id=? AND request_id=? FOR UPDATE",run.id,id);
            String status=string(row,"status");
            if(terminal(status)) { if("ISSUED".equals(status)) error(run,"replays");return; }
            long coupon=number(row,"coupon_id");
            jdbc.queryForMap("SELECT remaining FROM lab_coupons WHERE run_id=? AND coupon_id=? FOR UPDATE",run.id,coupon);
            Long duplicate=jdbc.queryForObject("SELECT COUNT(*) FROM lab_issued WHERE run_id=? AND coupon_id=? AND user_id=?",Long.class,run.id,coupon,row.get("user_id"));
            if(duplicate!=null&&duplicate>0) {
                if(flag(row,"db_reserved")) releaseReservation(run,row);
                reject(run,id,"DB_DUPLICATE_USER");return;
            }
            if(!flag(row,"db_reserved") && jdbc.update("UPDATE lab_coupons SET remaining=remaining-1 WHERE run_id=? AND coupon_id=? AND remaining>0",run.id,coupon)==0) {
                reject(run,id,"DB_CAPACITY_GUARD");return;
            }
            jdbc.update("INSERT INTO lab_issued VALUES(?,?,?,?)",run.id,coupon,row.get("user_id"),id);
            jdbc.update("UPDATE lab_coupons SET completion_seq=completion_seq+1 WHERE run_id=? AND coupon_id=?",run.id,coupon);
            Long sequence=jdbc.queryForObject("SELECT completion_seq FROM lab_coupons WHERE run_id=? AND coupon_id=?",Long.class,run.id,coupon);
            jdbc.update("UPDATE lab_requests SET status='ISSUED',accepted=TRUE,db_reserved=FALSE,completion_seq=?,completed_at=?,kafka_partition=?,kafka_offset=? WHERE run_id=? AND request_id=?",sequence,System.currentTimeMillis(),partition,offset,run.id,id);
        });
    }
    private void releaseReservation(Run run,Map<String,Object> row) {
        jdbc.update("UPDATE lab_coupons SET remaining=remaining+1 WHERE run_id=? AND coupon_id=?",run.id,row.get("coupon_id"));
        jdbc.update("UPDATE lab_requests SET db_reserved=FALSE WHERE run_id=? AND request_id=?",run.id,row.get("request_id"));
    }
    Map<String,Object> cancel(Run run,String id) {
        if(run.strategy!=Strategy.DB_OUTBOX) throw new LabException(400,"CANCEL_ONLY_DB_OUTBOX");
        tx.executeWithoutResult(s->{
            Map<String,Object> row=jdbc.queryForMap("SELECT * FROM lab_requests WHERE run_id=? AND request_id=? FOR UPDATE",run.id,id);
            String status=string(row,"status");
            if("ISSUED".equals(status)) throw new LabException(409,"ALREADY_ISSUED");
            if("CANCELLED".equals(status)) return;
            if(!flag(row,"db_reserved")) throw new LabException(409,"NO_LIVE_RESERVATION");
            releaseReservation(run,row);
            jdbc.update("UPDATE lab_requests SET status='CANCELLED',reason='CANCELLED',completed_at=? WHERE run_id=? AND request_id=?",System.currentTimeMillis(),run.id,id);
            jdbc.update("UPDATE lab_outbox SET sent=TRUE WHERE run_id=? AND request_id=?",run.id,id);
        });
        return request(run.id,id);
    }
}
