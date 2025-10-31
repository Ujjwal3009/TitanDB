package com.titandb.recovery;

import com.titandb.core.DiskBPlusTree;
import com.titandb.storage.Page;
import com.titandb.storage.DiskManager;
import com.titandb.wal.LogRecord;
import com.titandb.wal.LogType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * ARIES Recovery - REDO PHASE
 *
 * Goes forward through log from FirstRedoLSN
 * Reapplies all committed changes
 *
 * KEY OPTIMIZATION: Check page LSN before redoing
 * if (page.LSN >= log.LSN) → skip (already applied!)
 */
public class RedoPhase {
    private static final Logger logger = LoggerFactory.getLogger(RedoPhase.class);

    private final DiskManager diskManager;
    private final Map<Integer, Long> dirtyPageTable;
    private long firstRedoLSN;

    public RedoPhase(DiskManager diskManager, Map<Integer, Long> dirtyPageTable, long firstRedoLSN) {
        this.diskManager = diskManager;
        this.dirtyPageTable = dirtyPageTable;
        this.firstRedoLSN = firstRedoLSN;
    }

    /**
     * Execute REDO phase
     */
    public void redo(List<LogRecord> logRecords, Set<Integer> committedTxns) throws IOException {
        logger.info("═══════════════════════════════════════════════");
        logger.info("ARIES PHASE 2: REDO");
        logger.info("═══════════════════════════════════════════════");

        if (firstRedoLSN < 0) {
            logger.info("No dirty pages - skipping REDO phase");
            logger.info("═══════════════════════════════════════════════");
            return;
        }

        logger.info("Starting REDO from LSN: {}", firstRedoLSN);

        int redoCount = 0;
        int skippedCount = 0;

        for (LogRecord record : logRecords) {
            long lsn = record.getLsn();

            // Skip records before first redo LSN
            if (lsn < firstRedoLSN) {
                continue;
            }

            int txnId = record.getTransactionId();
            LogType type = record.getType();
            int pageId = record.getPageId();

            // Only redo records from committed transactions
            if (!committedTxns.contains(txnId)) {
                logger.debug("Skip LSN={} (txn {} not committed)", lsn, txnId);
                continue;
            }

            // Skip non-data operations
            if (type == LogType.BEGIN || type == LogType.COMMIT ||
                    type == LogType.ABORT || pageId < 0) {
                logger.debug("Skip LSN={} (control record)", lsn);
                continue;
            }

            // THIS IS THE KEY OPTIMIZATION!
            if (shouldRedo(pageId, lsn)) {
                logger.info("REDO LSN={}: {} page={}", lsn, type, pageId);

                // Read page from disk
                Page page = diskManager.readPage(pageId);

                // Apply the change
                applyChange(page, record);

                // Update page LSN
                page.setPageLSN(lsn);

                // Flush to disk
                diskManager.writePage(pageId, page);

                redoCount++;
            } else {
                logger.debug("Skip LSN={} (already on disk, page.LSN >= {})", lsn, lsn);
                skippedCount++;
            }
        }

        logger.info("");
        logger.info("REDO RESULTS:");
        logger.info("─────────────────────────────────────────────────");
        logger.info("REDO operations applied: {}", redoCount);
        logger.info("REDO operations skipped: {}", skippedCount);
        logger.info("═══════════════════════════════════════════════");
    }

    /**
     * Check if we should redo this record
     *
     * KEY: Use page LSN to determine if change is already on disk
     */
    private boolean shouldRedo(int pageId, long logLSN) throws IOException {
        // If page not in dirty page table, might already be correct
        if (!dirtyPageTable.containsKey(pageId)) {
            // Page was never marked dirty - but might have been written anyway
            // Read from disk to check LSN
            Page page = diskManager.readPage(pageId);

            // If page.LSN >= logLSN, change is already there
            if (page.getPageLSN() >= logLSN) {
                return false;  // Skip redo
            }
        }

        return true;  // Do redo
    }

    /**
     * Apply a change from log record to page
     * For now, just update page content
     */
    private void applyChange(Page page, LogRecord record) {
        // This is simplified - in real DB, would deserialize and apply actual change
        logger.debug("  ├─ Applied {} to page {}", record.getType(), record.getPageId());
        // page content would be updated from record.getNewValue()
    }
}
