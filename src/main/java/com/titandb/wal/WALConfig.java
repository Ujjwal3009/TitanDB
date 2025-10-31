package com.titandb.wal;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Configuration for Write-Ahead Log (WAL) system.
 *
 * Defines all tunable parameters for WAL behavior.
 * Production databases expose these as configuration files.
 */
public class WALConfig {

    /**
     * Directory where WAL files are stored.
     * Should be on a fast disk (SSD preferred).
     */
    private final Path walDirectory;

    /**
     * Maximum size of each WAL segment file (in bytes).
     * PostgreSQL default: 16 MB
     * MySQL default: 48 MB
     *
     * We use 16 MB for compatibility.
     */
    private final long segmentSize;

    /**
     * Size of in-memory log buffer (in bytes).
     * Records are batched here before flushing to disk.
     *
     * Larger buffer = better throughput, but more data loss on crash.
     * Smaller buffer = worse throughput, but less data loss.
     *
     * Default: 1 MB (good balance)
     */
    private final int logBufferSize;

    /**
     * Magic number written to file header for validation.
     * Used to detect corrupted or non-WAL files.
     */
    private static final int MAGIC_NUMBER = 0xDEADBEEF;

    /**
     * WAL file format version.
     * Incremented when format changes (for backward compatibility).
     */
    private static final int VERSION = 1;

    /**
     * Create WAL configuration with custom settings.
     */
    public WALConfig(Path walDirectory, long segmentSize, int logBufferSize) {
        this.walDirectory = walDirectory;
        this.segmentSize = segmentSize;
        this.logBufferSize = logBufferSize;
    }

    /**
     * Create default WAL configuration.
     *
     * Defaults:
     * - Directory: ./data/wal/
     * - Segment size: 16 MB
     * - Buffer size: 1 MB
     */
    public static WALConfig getDefault() {
        return new WALConfig(
                Paths.get("data", "wal"),
                16 * 1024 * 1024,  // 16 MB segments
                1024 * 1024        // 1 MB buffer
        );
    }

    // Getters

    public Path getWalDirectory() {
        return walDirectory;
    }

    public long getSegmentSize() {
        return segmentSize;
    }

    public int getLogBufferSize() {
        return logBufferSize;
    }

    public static int getMagicNumber() {
        return MAGIC_NUMBER;
    }

    public static int getVersion() {
        return VERSION;
    }

    /**
     * Generate segment filename from segment number.
     * Format: 000000000000000000000001.log (24 digits)
     *
     * Example:
     *   segmentNumber=1  → "000000000000000000000001.log"
     *   segmentNumber=42 → "000000000000000000000042.log"
     */
    public String getSegmentFileName(long segmentNumber) {
        return String.format("%024d.log", segmentNumber);
    }

    /**
     * Calculate which segment an LSN belongs to.
     *
     * Example (16 MB segments):
     *   LSN 0-16777215   → Segment 0
     *   LSN 16777216-... → Segment 1
     */
    public long getSegmentNumber(long lsn) {
        return lsn / segmentSize;
    }
}
