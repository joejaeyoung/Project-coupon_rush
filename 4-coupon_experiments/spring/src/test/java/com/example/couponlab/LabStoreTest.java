package com.example.couponlab;

import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.SimpleTransactionStatus;
import org.springframework.transaction.support.TransactionTemplate;

import static com.example.couponlab.LabModel.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class LabStoreTest {
    private final JdbcTemplate jdbc=mock(JdbcTemplate.class);
    private final PlatformTransactionManager transactions=mock(PlatformTransactionManager.class);
    private final LabStore store=new LabStore(jdbc,new TransactionTemplate(transactions));
    private final Run run=new Run("run","topic",Strategy.DB_OUTBOX,Ordering.BLOCKING,10,1,1,0,true);
    private final String requestId="request";

    @ParameterizedTest
    @ValueSource(strings={"ISSUED","REJECTED","CANCELLED"})
    void staleRetryCannotReserveARequestThatBecameTerminalBeforeTheLock(String status) {
        when(transactions.getTransaction(any())).thenReturn(new SimpleTransactionStatus());
        // Another API instance may finish this request after submit() reads RECEIVED.
        when(jdbc.queryForMap(contains("lab_requests"),eq(run.id),eq(requestId)))
            .thenReturn(Map.of("status",status,"db_reserved",false,"coupon_id",1L,"user_id","user"));
        // Stock is available again, for example after cancellation of a different request.
        when(jdbc.update(anyString(),any(Object[].class))).thenReturn(1);

        assertThat(store.reserveOutbox(run,requestId)).isFalse();
        verify(jdbc,never()).update(anyString(),any(Object[].class));
    }

    @Test
    void retryReusesALiveReservationWithoutDecrementingStockOrCreatingAnotherEvent() {
        when(transactions.getTransaction(any())).thenReturn(new SimpleTransactionStatus());
        when(jdbc.queryForMap(contains("lab_requests"),eq(run.id),eq(requestId)))
            .thenReturn(Map.of("status","RESERVED","db_reserved",true,"coupon_id",1L,"user_id","user"));

        assertThat(store.reserveOutbox(run,requestId)).isTrue();
        verify(jdbc,never()).update(anyString(),any(Object[].class));
    }
}
