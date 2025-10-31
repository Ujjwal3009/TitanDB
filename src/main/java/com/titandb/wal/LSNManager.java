package com.titandb.wal;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Manages Log Sequence Numbers (LSN) for the database.
 *
 * LSN is a globally unique, monotonically increasing 64-bit number
 * that serves as both:
 * 1. Unique identifier for each log record
 * 2. Logical timestamp (higher LSN = later in time)
 *
 * Thread-safe: Multiple threads can request LSNs concurrently.
 *
 * Example usage:
 *   LSNManager lsnManager = new LSNManager();
 *   long lsn1 = lsnManager.nextLSN(); // 1
 *   long lsn2 = lsnManager.nextLSN(); // 2
 *   long lsn3 = lsnManager.nextLSN(); // 3
 */
public class LSNManager {

    /**
     * Current LSN value.
     * AtomicLong provides thread-safe increment without locks.
     * Starts at 0, first nextLSN() returns 1.
     */
    private final AtomicLong currentLSN;

    /**
     * Create a new LSNManager starting from LSN 0.
     */
    public LSNManager() {
        this.currentLSN = new AtomicLong(0);
    }

    /**
     * Create LSNManager with a specific starting LSN.
     * Used during recovery to resume from last known LSN.
     *
     * @param startLSN The initial LSN value
     */
    public LSNManager(long startLSN) {
        if (startLSN < 0) {
            throw new IllegalArgumentException("Starting LSN cannot be negative");
        }
        this.currentLSN = new AtomicLong(startLSN);
    }

    /**
     * Get the next LSN.
     *
     * This is THE critical method - every change to the database
     * gets a unique LSN by calling this.
     *
     * Thread-safe: Uses AtomicLong.incrementAndGet() which is
     * lock-free and very fast (~10 nanoseconds).
     *
     * @return A new, unique LSN
     */
    public long nextLSN() {
        return currentLSN.incrementAndGet();
    }

    /**
     * Get the current LSN value without incrementing.
     * Used to check "what's the latest LSN?" without consuming one.
     *
     * @return Current LSN value
     */
    public long getCurrentLSN() {
        return currentLSN.get();
    }

    /**
     * Reset LSN to a specific value.
     *
     * WARNING: Only use during testing or recovery!
     * In production, LSN should only increase.
     *
     * @param lsn The new LSN value
     */
    public void setLSN(long lsn) {
        if (lsn < 0) {
            throw new IllegalArgumentException("LSN cannot be negative");
        }
        currentLSN.set(lsn);
    }
}
