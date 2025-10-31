package com.titandb.concurrency;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * MVCC Transaction Manager
 */
public class TransactionManager {

    private final AtomicLong nextTxnId = new AtomicLong(1);
    private final Map<Long, TransactionState> activeTransactions =
            Collections.synchronizedMap(new HashMap<>());

    enum TransactionState {
        RUNNING,
        COMMITTED,
        ABORTED
    }

    /**
     * Transaction with snapshot start time
     */
    public static class Transaction {
        public final long txnId;
        public final long startTime;
        public final long startLSN;

        public Transaction(long txnId, long startTime, long startLSN) {
            this.txnId = txnId;
            this.startTime = startTime;
            this.startLSN = startLSN;
        }
    }

    public Transaction begin() {
        long txnId = nextTxnId.getAndIncrement();
        activeTransactions.put(txnId, TransactionState.RUNNING);
        System.out.println("✅ Started Txn " + txnId);
        return new Transaction(txnId, System.currentTimeMillis(), txnId);
    }

    public void commit(long txnId) {
        activeTransactions.put(txnId, TransactionState.COMMITTED);
        System.out.println("✅ Committed Txn " + txnId);
    }

    public void abort(long txnId) {
        activeTransactions.put(txnId, TransactionState.ABORTED);
        System.out.println("❌ Aborted Txn " + txnId);
    }

    public boolean isRunning(long txnId) {
        return activeTransactions.get(txnId) == TransactionState.RUNNING;
    }

    public boolean isCommitted(long txnId) {
        return activeTransactions.get(txnId) == TransactionState.COMMITTED;
    }

    public Map<Long, TransactionState> getActiveTransactions() {
        return new HashMap<>(activeTransactions);
    }
}
