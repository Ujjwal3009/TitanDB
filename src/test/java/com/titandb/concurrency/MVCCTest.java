package com.titandb.concurrency;

import org.junit.jupiter.api.*;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("MVCC Concurrency Control")
public class MVCCTest {

    private TransactionManager tm;

    @BeforeEach
    void setUp() {
        tm = new TransactionManager();
    }

    @Test
    @DisplayName("Transaction manager tracks state")
    void testTransactionTracking() {
        System.out.println("\n✅ Test: Transaction Tracking");

        var txn1 = tm.begin();
        assertEquals(true, tm.isRunning(txn1.txnId));

        tm.commit(txn1.txnId);
        assertEquals(true, tm.isCommitted(txn1.txnId));

        var txn2 = tm.begin();
        tm.abort(txn2.txnId);
        assertEquals(false, tm.isRunning(txn2.txnId));
    }

    @Test
    @DisplayName("Multiple transactions tracked independently")
    void testMultipleTransactions() {
        System.out.println("\n✅ Test: Multiple Transactions");

        var txn1 = tm.begin();
        var txn2 = tm.begin();
        var txn3 = tm.begin();

        tm.commit(txn1.txnId);
        tm.abort(txn2.txnId);

        assertEquals(true, tm.isCommitted(txn1.txnId));
        assertEquals(false, tm.isRunning(txn2.txnId));
        assertEquals(true, tm.isRunning(txn3.txnId));
    }
}
