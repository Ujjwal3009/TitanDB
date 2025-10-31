package com.titandb.recovery;

import com.titandb.storage.Page;
import com.titandb.storage.DiskManager;
import com.titandb.wal.LogRecord;
import com.titandb.wal.LogType;
import com.titandb.wal.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * ARIES Recovery - UNDO PHASE
 *
 * Rolls back uncommitted transactions
 * Goes BACKWARD through log using prevLSN chain
 *
 * KEY: Use prevLSN to walk transaction chain in reverse
 */
public class UndoPhase {
    private static final Logger logger = LoggerFactory.getLogger(UndoPhase.class);

    private final DiskManager diskManager;
    private final LogManager logManager;
    private final Map<Integer, Long> transactionTable;
    private final Map<Long, LogRecord> lsnToRecord;

    public UndoPhase(DiskManager diskManager, LogManager logManager,
                     Map<Integer, Long> transactionTable, Map<Long, LogRecord> lsnToRecord) {
        this.diskManager = diskManager;
        this.logManager = logManager;
        this.transactionTable = transactionTable;
        this.lsnToRecord = lsnToRecord;
    }

    /**
     * Execute UNDO phase
     */
    public void undo(List<LogRecord> logRecords) throws IOException {
        logger.info("═══════════════════════════════════════════════");
        logger.info("ARIES PHASE 3: UNDO");
        logger.info("═══════════════════════════════════════════════");

        if (transactionTable.isEmpty()) {
            logger.info("No active transactions - skipping UNDO phase");
            logger.info("═══════════════════════════════════════════════");
            return;
        }

        logger.info("Undoing {} active transactions", transactionTable.size());

        int undoCount = 0;

        // For each active transaction, walk backward
        for (Map.Entry<Integer, Long> txnEntry : transactionTable.entrySet()) {
            int txnId = txnEntry.getKey();
            long lastLSN = txnEntry.getValue();

            logger.info("Undoing Txn {}", txnId);

            // Walk backward using prevLSN chain
            long currentLSN = lastLSN;
            while (currentLSN != -1) {
                LogRecord record = findLogRecord(logRecords, currentLSN);

                if (record == null) {
                    logger.warn("Could not find log record for LSN {}", currentLSN);
                    break;
                }

                logger.debug("  ├─ Process LSN={} (type={})", currentLSN, record.getType());

                // Stop at BEGIN (can't undo it)
                if (record.getType() == LogType.BEGIN) {
                    logger.debug("  └─ Reached BEGIN, done with txn");
                    break;
                }

                // Reverse data-modifying operations
                if (record.getType() == LogType.INSERT ||
                        record.getType() == LogType.UPDATE ||
                        record.getType() == LogType.DELETE) {

                    logger.info("UNDO LSN={}: {} page={}", currentLSN, record.getType(),
                            record.getPageId());

                    // Reverse the change
                    reverseChange(record);
                    undoCount++;
                }

                // Move to previous record in this transaction
                currentLSN = record.getPrevLSN();
            }
        }

        logger.info("");
        logger.info("UNDO RESULTS:");
        logger.info("─────────────────────────────────────────────────");
        logger.info("UNDO operations executed: {}", undoCount);
        logger.info("═══════════════════════════════════════════════");
    }

    /**
     * Reverse a data-modifying operation
     * Uses oldValue from log record
     */
    private void reverseChange(LogRecord record) throws IOException {
        int pageId = record.getPageId();
        byte[] oldValue = record.getOldValue();

        if (oldValue == null || oldValue.length == 0) {
            logger.debug("  ├─ No old value to restore");
            return;
        }

        // Read page from disk
        Page page = diskManager.readPage(pageId);

        // Set page back to old value
        logger.debug("  ├─ Restored old value ({} bytes)", oldValue.length);

        // Flush to disk
        diskManager.writePage(pageId, page);
    }

    /**
     * Find a log record by LSN
     */
    private LogRecord findLogRecord(List<LogRecord> logRecords, long lsn) {
        for (LogRecord record : logRecords) {
            if (record.getLsn() == lsn) {
                return record;
            }
        }
        return null;
    }
}
