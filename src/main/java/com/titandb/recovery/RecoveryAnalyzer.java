package com.titandb.recovery;

import com.titandb.wal.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * ARIES Recovery - ANALYSIS PHASE
 *
 * Scans log forward to find:
 * 1. Which pages are dirty (DirtyPageTable)
 * 2. Which transactions are active (TransactionTable)
 * 3. Where to start REDO (FirstRedoLSN)
 */
public class RecoveryAnalyzer {
    private static final Logger logger = LoggerFactory.getLogger(RecoveryAnalyzer.class);

    /**
     * Maps page_id → first LSN that dirtied it
     */
    private final Map<Integer, Long> dirtyPageTable = new HashMap<>();

    /**
     * Maps txn_id → last LSN of that transaction
     */
    private final Map<Integer, Long> transactionTable = new HashMap<>();

    /**
     * First LSN where REDO needs to start
     */
    private long firstRedoLSN = Long.MAX_VALUE;

    /**
     * Run analysis phase on log
     */
    public void analyze(List<LogRecord> logRecords) {
        logger.info("═══════════════════════════════════════════════");
        logger.info("ARIES PHASE 1: ANALYSIS");
        logger.info("═══════════════════════════════════════════════");
        logger.info("Scanning {} log records...", logRecords.size());

        dirtyPageTable.clear();
        transactionTable.clear();
        firstRedoLSN = Long.MAX_VALUE;

        int recordCount = 0;
        for (LogRecord record : logRecords) {
            recordCount++;

            long lsn = record.getLsn();
            int txnId = record.getTransactionId();
            int pageId = record.getPageId();

            logger.debug("Analyzing LSN={}, Txn={}, Type={}, Page={}",
                    lsn, txnId, record.getType(), pageId);

            // Update transaction table
            if (record.getType() == LogType.BEGIN) {
                transactionTable.put(txnId, lsn);
                logger.debug("  ├─ BEGIN Txn {}", txnId);
            }
            else if (record.getType() == LogType.COMMIT) {
                transactionTable.remove(txnId);
                logger.debug("  ├─ COMMIT Txn {} (removed from active)", txnId);
            }
            else if (record.getType() == LogType.ABORT) {
                transactionTable.remove(txnId);
                logger.debug("  ├─ ABORT Txn {} (removed from active)", txnId);
            }
            else {
                // UPDATE, INSERT, DELETE - mark page as dirty
                if (pageId >= 0) {
                    if (!dirtyPageTable.containsKey(pageId)) {
                        dirtyPageTable.put(pageId, lsn);
                        firstRedoLSN = Math.min(firstRedoLSN, lsn);
                        logger.debug("  ├─ Mark page {} DIRTY at LSN {}", pageId, lsn);
                    } else {
                        logger.debug("  ├─ Page {} already dirty (first LSN: {})",
                                pageId, dirtyPageTable.get(pageId));
                    }
                }

                // Update transaction table with latest LSN
                transactionTable.put(txnId, lsn);
            }
        }

        // Print analysis results
        logger.info("");
        logger.info("ANALYSIS RESULTS:");
        logger.info("─────────────────────────────────────────────────");
        logger.info("Processed {} records", recordCount);
        logger.info("Dirty Pages: {} pages", dirtyPageTable.size());

        for (Map.Entry<Integer, Long> entry : dirtyPageTable.entrySet()) {
            logger.info("  ├─ Page {} first dirtied at LSN {}", entry.getKey(), entry.getValue());
        }

        logger.info("Active Transactions: {} txns", transactionTable.size());
        for (Map.Entry<Integer, Long> entry : transactionTable.entrySet()) {
            logger.info("  ├─ Txn {} (last LSN: {})", entry.getKey(), entry.getValue());
        }

        logger.info("First REDO LSN: {}", firstRedoLSN == Long.MAX_VALUE ? "N/A (nothing to redo)" : firstRedoLSN);
        logger.info("═══════════════════════════════════════════════");
    }

    // Getters
    public Map<Integer, Long> getDirtyPageTable() {
        return dirtyPageTable;
    }

    public Map<Integer, Long> getTransactionTable() {
        return transactionTable;
    }

    public long getFirstRedoLSN() {
        return firstRedoLSN == Long.MAX_VALUE ? -1 : firstRedoLSN;
    }
}
