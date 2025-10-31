package com.titandb.recovery;

import com.titandb.wal.*;
import org.junit.jupiter.api.*;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("ARIES Recovery - Complete Test Suite")
public class ARIESRecoveryTest {

    @Test
    @DisplayName("Phase 1: Analysis Phase - Simple Committed Transaction")
    void testAnalysisSimpleCommit() {
        RecoveryAnalyzer analyzer = new RecoveryAnalyzer();

        List<LogRecord> records = new ArrayList<>();
        records.add(LogRecord.builder().lsn(1).transactionId(100)
                .type(LogType.BEGIN).pageId(-1).build());
        records.add(LogRecord.builder().lsn(2).transactionId(100).prevLSN(1)
                .type(LogType.INSERT).pageId(1).build());
        records.add(LogRecord.builder().lsn(3).transactionId(100).prevLSN(2)
                .type(LogType.COMMIT).pageId(-1).build());

        analyzer.analyze(records);

        assertEquals(1, analyzer.getDirtyPageTable().size());
        assertEquals(2L, analyzer.getDirtyPageTable().get(1));
        assertEquals(0, analyzer.getTransactionTable().size());
        assertEquals(2L, analyzer.getFirstRedoLSN());
    }

    @Test
    @DisplayName("Phase 1: Analysis Phase - Mixed Committed and Uncommitted")
    void testAnalysisMixedTransactions() {
        RecoveryAnalyzer analyzer = new RecoveryAnalyzer();

        List<LogRecord> records = new ArrayList<>();

        // Committed txn
        records.add(LogRecord.builder().lsn(1).transactionId(100)
                .type(LogType.BEGIN).pageId(-1).build());
        records.add(LogRecord.builder().lsn(2).transactionId(100).prevLSN(1)
                .type(LogType.INSERT).pageId(1).build());
        records.add(LogRecord.builder().lsn(3).transactionId(100).prevLSN(2)
                .type(LogType.COMMIT).pageId(-1).build());

        // Uncommitted txn
        records.add(LogRecord.builder().lsn(4).transactionId(200)
                .type(LogType.BEGIN).pageId(-1).build());
        records.add(LogRecord.builder().lsn(5).transactionId(200).prevLSN(4)
                .type(LogType.UPDATE).pageId(2).build());

        analyzer.analyze(records);

        assertEquals(2, analyzer.getDirtyPageTable().size());
        assertEquals(1, analyzer.getTransactionTable().size());
        assertTrue(analyzer.getTransactionTable().containsKey(200));
        assertEquals(5L, analyzer.getTransactionTable().get(200));
    }

    @Test
    @DisplayName("Phase 1: Analysis Phase - First LSN Tracking")
    void testAnalysisFirstLSNTracking() {
        RecoveryAnalyzer analyzer = new RecoveryAnalyzer();

        List<LogRecord> records = new ArrayList<>();
        records.add(LogRecord.builder().lsn(1).transactionId(100)
                .type(LogType.BEGIN).pageId(-1).build());
        records.add(LogRecord.builder().lsn(2).transactionId(100).prevLSN(1)
                .type(LogType.INSERT).pageId(1).build());
        records.add(LogRecord.builder().lsn(3).transactionId(100).prevLSN(2)
                .type(LogType.UPDATE).pageId(1).build());
        records.add(LogRecord.builder().lsn(4).transactionId(100).prevLSN(3)
                .type(LogType.UPDATE).pageId(1).build());

        analyzer.analyze(records);

        // Should record FIRST LSN that dirtied the page
        assertEquals(2L, analyzer.getDirtyPageTable().get(1));
        assertEquals(2L, analyzer.getFirstRedoLSN());
    }

    @Test
    @DisplayName("Phase 1: Analysis Phase - Multiple Dirty Pages")
    void testAnalysisMultipleDirtyPages() {
        RecoveryAnalyzer analyzer = new RecoveryAnalyzer();

        List<LogRecord> records = new ArrayList<>();
        records.add(LogRecord.builder().lsn(1).transactionId(100)
                .type(LogType.BEGIN).pageId(-1).build());
        records.add(LogRecord.builder().lsn(2).transactionId(100).prevLSN(1)
                .type(LogType.INSERT).pageId(1).build());
        records.add(LogRecord.builder().lsn(3).transactionId(100).prevLSN(2)
                .type(LogType.INSERT).pageId(2).build());
        records.add(LogRecord.builder().lsn(4).transactionId(100).prevLSN(3)
                .type(LogType.INSERT).pageId(3).build());

        analyzer.analyze(records);

        assertEquals(3, analyzer.getDirtyPageTable().size());
        assertEquals(2L, analyzer.getFirstRedoLSN());  // First page dirtied at LSN 2
    }

    @Test
    @DisplayName("prevLSN Chain - Transaction Undo Chain Integrity")
    void testPrevLSNChainIntegrity() {
        List<LogRecord> records = new ArrayList<>();

        records.add(LogRecord.builder().lsn(1).transactionId(100)
                .type(LogType.BEGIN).pageId(-1).build());
        records.add(LogRecord.builder().lsn(2).transactionId(100).prevLSN(1)
                .type(LogType.INSERT).pageId(1).build());
        records.add(LogRecord.builder().lsn(3).transactionId(100).prevLSN(2)
                .type(LogType.UPDATE).pageId(1).build());
        records.add(LogRecord.builder().lsn(4).transactionId(100).prevLSN(3)
                .type(LogType.COMMIT).pageId(-1).build());

        // Verify backward chain: 4 → 3 → 2 → 1 → -1
        assertEquals(-1, records.get(0).getPrevLSN());
        assertEquals(1, records.get(1).getPrevLSN());
        assertEquals(2, records.get(2).getPrevLSN());
        assertEquals(3, records.get(3).getPrevLSN());
    }

    @Test
    @DisplayName("Log Type Classification - Committed Transactions")
    void testCommittedTransactionIdentification() {
        List<LogRecord> records = new ArrayList<>();

        records.add(LogRecord.builder().lsn(1).transactionId(100)
                .type(LogType.BEGIN).pageId(-1).build());
        records.add(LogRecord.builder().lsn(2).transactionId(100).prevLSN(1)
                .type(LogType.INSERT).pageId(1).build());
        records.add(LogRecord.builder().lsn(3).transactionId(100).prevLSN(2)
                .type(LogType.COMMIT).pageId(-1).build());

        // Count commits
        long commits = records.stream()
                .filter(r -> r.getType() == LogType.COMMIT)
                .count();

        assertEquals(1, commits);
    }

    @Test
    @DisplayName("Dirty Page Table - Correctness")
    void testDirtyPageTableCorrectness() {
        RecoveryAnalyzer analyzer = new RecoveryAnalyzer();

        List<LogRecord> records = new ArrayList<>();
        records.add(LogRecord.builder().lsn(1).transactionId(100)
                .type(LogType.BEGIN).pageId(-1).build());
        records.add(LogRecord.builder().lsn(10).transactionId(100).prevLSN(1)
                .type(LogType.INSERT).pageId(5).build());
        records.add(LogRecord.builder().lsn(20).transactionId(100).prevLSN(10)
                .type(LogType.UPDATE).pageId(5).build());
        records.add(LogRecord.builder().lsn(30).transactionId(100).prevLSN(20)
                .type(LogType.UPDATE).pageId(10).build());

        analyzer.analyze(records);

        // DirtyPageTable should map pages to their FIRST LSN
        assertEquals(10L, analyzer.getDirtyPageTable().get(5));   // First time page 5 was dirtied
        assertEquals(30L, analyzer.getDirtyPageTable().get(10));  // First and only time page 10 was dirtied
    }
}
