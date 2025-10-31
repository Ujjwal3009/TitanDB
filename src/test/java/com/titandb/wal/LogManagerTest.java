package com.titandb.wal;

import org.junit.jupiter.api.*;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Comparator;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for LogManager.
 */
@DisplayName("LogManager Tests")
public class LogManagerTest {

    private Path tempDir;
    private WALConfig config;
    private LSNManager lsnManager;
    private LogManager logManager;

    @BeforeEach
    void setUp() throws IOException {
        // Create temporary directory for WAL files
        tempDir = Files.createTempDirectory("wal-test");

        config = new WALConfig(
                tempDir,
                16 * 1024 * 1024,  // 16 MB segments
                1024               // 1 KB buffer (small for testing)
        );

        lsnManager = new LSNManager();
        logManager = new LogManager(config, lsnManager);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (logManager != null) {
            logManager.close();
        }

        // Delete temporary directory
        if (tempDir != null && Files.exists(tempDir)) {
            Files.walk(tempDir)
                    .sorted(Comparator.reverseOrder())
                    .forEach(path -> {
                        try {
                            Files.delete(path);
                        } catch (IOException e) {
                            // Ignore
                        }
                    });
        }
    }

    @Test
    @DisplayName("Append single log record")
    void testAppendSingleRecord() throws IOException {
        LogRecord record = LogRecord.builder()
                .lsn(lsnManager.nextLSN())
                .transactionId(1)
                .type(LogType.BEGIN)
                .pageId(-1)
                .build();

        long lsn = logManager.appendLogRecord(record, true);

        assertEquals(record.getLsn(), lsn);
        assertEquals(lsn, logManager.getFlushedLSN());
    }

    @Test
    @DisplayName("Append multiple records and read back")
    void testAppendAndRead() throws IOException {
        // Append 10 records
        for (int i = 0; i < 10; i++) {
            LogRecord record = LogRecord.builder()
                    .lsn(lsnManager.nextLSN())
                    .transactionId(i)
                    .type(LogType.INSERT)
                    .pageId(i)
                    .newValue(("value" + i).getBytes())
                    .build();

            logManager.appendLogRecord(record, false);
        }

        logManager.flush();

        // Read back
        List<LogRecord> records = logManager.readAllLogRecords();

        assertEquals(10, records.size());

        // Verify order and content
        for (int i = 0; i < 10; i++) {
            LogRecord record = records.get(i);
            assertEquals(i + 1, record.getLsn());
            assertEquals(i, record.getTransactionId());
            assertEquals(LogType.INSERT, record.getType());
            assertArrayEquals(("value" + i).getBytes(), record.getNewValue());
        }
    }

    @Test
    @DisplayName("Force flush on COMMIT")
    void testForceFlush() throws IOException {
        // Append without force
        LogRecord insert = LogRecord.builder()
                .lsn(lsnManager.nextLSN())
                .transactionId(1)
                .type(LogType.INSERT)
                .pageId(10)
                .newValue("data".getBytes())
                .build();

        logManager.appendLogRecord(insert, false);

        // Not flushed yet
        long flushedBefore = logManager.getFlushedLSN();

        // Commit with force
        LogRecord commit = LogRecord.builder()
                .lsn(lsnManager.nextLSN())
                .transactionId(1)
                .prevLSN(insert.getLsn())
                .type(LogType.COMMIT)
                .pageId(-1)
                .build();

        logManager.appendLogRecord(commit, true);

        // Now flushed
        assertEquals(commit.getLsn(), logManager.getFlushedLSN());
        assertTrue(logManager.getFlushedLSN() > flushedBefore);
    }

    @Test
    @DisplayName("Large batch write")
    void testLargeBatch() throws IOException {
        int numRecords = 1000;

        for (int i = 0; i < numRecords; i++) {
            LogRecord record = LogRecord.builder()
                    .lsn(lsnManager.nextLSN())
                    .transactionId(i % 10)
                    .type(LogType.UPDATE)
                    .pageId(i)
                    .oldValue(("old" + i).getBytes())
                    .newValue(("new" + i).getBytes())
                    .build();

            logManager.appendLogRecord(record, false);
        }

        logManager.flush();

        List<LogRecord> records = logManager.readAllLogRecords();
        assertEquals(numRecords, records.size());
    }

    @Test
    @DisplayName("Recovery - read existing WAL")
    void testRecovery() throws IOException {
        // Write some records
        for (int i = 0; i < 5; i++) {
            LogRecord record = LogRecord.builder()
                    .lsn(lsnManager.nextLSN())
                    .transactionId(1)
                    .type(LogType.INSERT)
                    .pageId(i)
                    .newValue(("value" + i).getBytes())
                    .build();

            logManager.appendLogRecord(record, true);
        }

        logManager.close();

        // Simulate crash and restart
        LSNManager newLsnManager = new LSNManager(5); // Resume from LSN 5
        LogManager newLogManager = new LogManager(config, newLsnManager);

        // Read existing records
        List<LogRecord> records = newLogManager.readAllLogRecords();

        assertEquals(5, records.size());
        for (int i = 0; i < 5; i++) {
            assertEquals(i + 1, records.get(i).getLsn());
        }

        newLogManager.close();
    }

    @Test
    @DisplayName("WAL file created in correct directory")
    void testWALFileLocation() throws IOException {
        LogRecord record = LogRecord.builder()
                .lsn(lsnManager.nextLSN())
                .transactionId(1)
                .type(LogType.BEGIN)
                .pageId(-1)
                .build();

        logManager.appendLogRecord(record, true);

        // Check file exists
        Path segmentPath = tempDir.resolve("000000000000000000000000.log");
        assertTrue(Files.exists(segmentPath));
        assertTrue(Files.size(segmentPath) > 0);
    }

    @Test
    @DisplayName("DEBUG: Single record test")
    void testSingleRecord() throws IOException {
        LogRecord record = LogRecord.builder()
                .lsn(lsnManager.nextLSN())
                .transactionId(1)
                .type(LogType.BEGIN)
                .pageId(-1)
                .build();

        logManager.appendLogRecord(record, true);

        List<LogRecord> records = logManager.readAllLogRecords();

        System.out.println("\n========== ASSERTION ==========");
        System.out.println("Expected: 1, Actual: " + records.size());

        assertEquals(1, records.size());
    }


    @Test
    @DisplayName("Transaction chain with prevLSN")
    void testTransactionChain() throws IOException {
        long beginLSN = lsnManager.nextLSN();
        LogRecord begin = LogRecord.builder()
                .lsn(beginLSN)
                .transactionId(77)
                .type(LogType.BEGIN)
                .pageId(-1)
                .build();
        logManager.appendLogRecord(begin, false);

        long insertLSN = lsnManager.nextLSN();
        LogRecord insert = LogRecord.builder()
                .lsn(insertLSN)
                .transactionId(77)
                .prevLSN(beginLSN)
                .type(LogType.INSERT)
                .pageId(10)
                .newValue("data".getBytes())
                .build();
        logManager.appendLogRecord(insert, false);

        long commitLSN = lsnManager.nextLSN();
        LogRecord commit = LogRecord.builder()
                .lsn(commitLSN)
                .transactionId(77)
                .prevLSN(insertLSN)
                .type(LogType.COMMIT)
                .pageId(-1)
                .build();
        logManager.appendLogRecord(commit, true);

        // Read and verify chain
        List<LogRecord> records = logManager.readAllLogRecords();

        assertEquals(3, records.size());
        assertEquals(-1, records.get(0).getPrevLSN());       // BEGIN has no prev
        assertEquals(beginLSN, records.get(1).getPrevLSN()); // INSERT → BEGIN
        assertEquals(insertLSN, records.get(2).getPrevLSN());// COMMIT → INSERT
    }
}
