package com.titandb.recovery;

import com.titandb.core.DiskBPlusTree;
import com.titandb.storage.DiskManager;
import com.titandb.wal.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * ARIES Recovery Manager
 *
 * Orchestrates the complete recovery process:
 * 1. ANALYSIS: Find dirty pages & active transactions
 * 2. REDO: Reapply committed changes
 * 3. UNDO: Rollback uncommitted changes
 */
public class RecoveryManager {
    private static final Logger logger = LoggerFactory.getLogger(RecoveryManager.class);

    private final DiskManager diskManager;
    private final LogManager logManager;

    public RecoveryManager(DiskManager diskManager, LogManager logManager) {
        this.diskManager = diskManager;
        this.logManager = logManager;
    }

    /**
     * Execute complete ARIES recovery
     */
    public void recover() throws IOException {
        logger.info("");
        logger.info("╔════════════════════════════════════════════════╗");
        logger.info("║         ARIES CRASH RECOVERY STARTING          ║");
        logger.info("╚════════════════════════════════════════════════╝");
        logger.info("");

        // Read all log records from disk
        List<LogRecord> logRecords = logManager.readAllLogRecords();
        logger.info("Read {} log records from WAL", logRecords.size());

        // PHASE 1: ANALYSIS
        RecoveryAnalyzer analyzer = new RecoveryAnalyzer();
        analyzer.analyze(logRecords);

        // PHASE 2: REDO
        RedoPhase redoPhase = new RedoPhase(
                diskManager,
                analyzer.getDirtyPageTable(),
                analyzer.getFirstRedoLSN()
        );

        // Identify committed transactions
        Set<Integer> committedTxns = findCommittedTransactions(logRecords);
        redoPhase.redo(logRecords, committedTxns);

        // PHASE 3: UNDO
        UndoPhase undoPhase = new UndoPhase(
                diskManager,
                logManager,
                analyzer.getTransactionTable(),
                null  // lsnToRecord map not needed for basic implementation
        );
        undoPhase.undo(logRecords);

        logger.info("");
        logger.info("╔════════════════════════════════════════════════╗");
        logger.info("║     ARIES CRASH RECOVERY COMPLETED ✅          ║");
        logger.info("╚════════════════════════════════════════════════╝");
        logger.info("");
    }

    /**
     * Find all committed transactions
     */
    private Set<Integer> findCommittedTransactions(List<LogRecord> logRecords) {
        Set<Integer> committed = new HashSet<>();

        for (LogRecord record : logRecords) {
            if (record.getType() == LogType.COMMIT) {
                committed.add(record.getTransactionId());
            }
        }

        return committed;
    }
}
