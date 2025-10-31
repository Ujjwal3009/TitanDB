package com.titandb.wal;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;


public class LogRecord {
    private static final int HEADER_SIZE = 37;

    /**
     * Log Sequence Number - globally unique, monotonically increasing.
     * Acts as both ID and timestamp (higher LSN = later in time).
     */

    private final long lsn;
    /**
     * ID of the transaction that created this log record.
     * Allows grouping all operations of a single transaction.
     */
    private final int transactionId;


    /**
     * LSN of the previous log record from the same transaction.
     * Forms a linked list for efficient rollback (undo chain).
     * null (represented as -1) for BEGIN records.
     */
    private final long prevLSN;

    /**
     * Type of operation (INSERT, UPDATE, DELETE, etc.).
     */
    private final LogType type;

    /**
     * ID of the page affected by this operation.
     * During recovery, we use this to find which page to update.
     */
    private final int pageId;

    /**
     * Old value (before the change).
     * Used during UNDO (rollback) to restore previous state.
     * null for INSERT operations (no previous value).
     */
    private final byte[] oldValue;

    /**
     * New value (after the change).
     * Used during REDO (crash recovery) to reapply the change.
     * null for DELETE operations (no new value).
     */
    private final byte[] newValue;

    /**
     * Checksum for corruption detection.
     * Simple CRC32 checksum of all fields.
     * If checksum doesn't match during read, log is corrupted.
     */
    private final int checksum;

    /**
     * Full constructor - used internally.
     */
    private LogRecord(long lsn, int transactionId, long prevLSN, LogType type,
                      int pageId, byte[] oldValue, byte[] newValue, int checksum) {
        this.lsn = lsn;
        this.transactionId = transactionId;
        this.prevLSN = prevLSN;
        this.type = Objects.requireNonNull(type, "LogType cannot be null");
        this.pageId = pageId;
        this.oldValue = oldValue;
        this.newValue = newValue;
        this.checksum = checksum;
    }

    // ==================== Builder Pattern ====================

    /**
     * Create a new LogRecord using the Builder pattern.
     * This makes construction clearer and allows optional fields.
     *
     * Example:
     *   LogRecord record = LogRecord.builder()
     *       .lsn(1042)
     *       .transactionId(77)
     *       .prevLSN(1038)
     *       .type(LogType.UPDATE)
     *       .pageId(512)
     *       .oldValue("balance=100".getBytes())
     *       .newValue("balance=150".getBytes())
     *       .build();
     */
    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private long lsn;
        private int transactionId;
        private long prevLSN = -1; // -1 means no previous LSN
        private LogType type;
        private int pageId;
        private byte[] oldValue;
        private byte[] newValue;

        public Builder lsn(long lsn) {
            this.lsn = lsn;
            return this;
        }

        public Builder transactionId(int transactionId) {
            this.transactionId = transactionId;
            return this;
        }

        public Builder prevLSN(long prevLSN) {
            this.prevLSN = prevLSN;
            return this;
        }

        public Builder type(LogType type) {
            this.type = type;
            return this;
        }

        public Builder pageId(int pageId) {
            this.pageId = pageId;
            return this;
        }

        public Builder oldValue(byte[] oldValue) {
            this.oldValue = oldValue;
            return this;
        }

        public Builder newValue(byte[] newValue) {
            this.newValue = newValue;
            return this;
        }

        public LogRecord build() {
            // Calculate checksum
            int checksum = calculateChecksum(lsn, transactionId, prevLSN, type,
                    pageId, oldValue, newValue);

            return new LogRecord(lsn, transactionId, prevLSN, type, pageId,
                    oldValue, newValue, checksum);
        }
    }

    // ==================== Serialization ====================

    /**
     * Convert this LogRecord to bytes for writing to disk.
     *
     * Format matches the structure diagram at top of class.
     * Uses ByteBuffer for efficient binary packing.
     *
     * @return Byte array representation of this log record
     */
    public byte[] serialize() {
        int oldLen = (oldValue != null) ? oldValue.length : 0;
        int newLen = (newValue != null) ? newValue.length : 0;
        int totalSize = HEADER_SIZE + oldLen + newLen;

        ByteBuffer buffer = ByteBuffer.allocate(totalSize);

        // Write header (fixed 37 bytes)
        buffer.putLong(lsn);                    // 8 bytes
        buffer.putInt(transactionId);           // 4 bytes
        buffer.putLong(prevLSN);                // 8 bytes
        buffer.put((byte) type.getTypeCode());  // 1 byte
        buffer.putInt(pageId);                  // 4 bytes

        // Write old value
        buffer.putInt(oldLen);                  // 4 bytes
        if (oldValue != null) {
            buffer.put(oldValue);               // variable
        }

        // Write new value
        buffer.putInt(newLen);                  // 4 bytes
        if (newValue != null) {
            buffer.put(newValue);               // variable
        }

        // Write checksum
        buffer.putInt(checksum);                // 4 bytes

        return buffer.array();
    }

    /**
     * Deserialize bytes back into a LogRecord object.
     *
     * This is the inverse of serialize() - used when reading from WAL file.
     * Validates checksum to detect corruption.
     *
     * @param bytes Serialized log record
     * @return Reconstructed LogRecord
     * @throws IllegalArgumentException if checksum is invalid
     */
    public static LogRecord deserialize(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);

        // Read header
        long lsn = buffer.getLong();
        int transactionId = buffer.getInt();
        long prevLSN = buffer.getLong();
        byte typeCode = buffer.get();
        LogType type = LogType.fromTypeCode(typeCode);
        int pageId = buffer.getInt();

        // Read old value
        int oldLen = buffer.getInt();
        byte[] oldValue = null;
        if (oldLen > 0) {
            oldValue = new byte[oldLen];
            buffer.get(oldValue);
        }

        // Read new value
        int newLen = buffer.getInt();
        byte[] newValue = null;
        if (newLen > 0) {
            newValue = new byte[newLen];
            buffer.get(newValue);
        }

        // Read and verify checksum
        int storedChecksum = buffer.getInt();
        int calculatedChecksum = calculateChecksum(lsn, transactionId, prevLSN,
                type, pageId, oldValue, newValue);

        if (storedChecksum != calculatedChecksum) {
            throw new IllegalArgumentException(
                    "Checksum mismatch! Log record may be corrupted. " +
                            "Stored: " + storedChecksum + ", Calculated: " + calculatedChecksum);
        }

        return new LogRecord(lsn, transactionId, prevLSN, type, pageId,
                oldValue, newValue, storedChecksum);
    }

    /**
     * Calculate CRC32-like checksum for corruption detection.
     *
     * This is a simplified checksum - production would use java.util.zip.CRC32.
     * For learning purposes, we use a simple hash-based approach.
     */
    private static int calculateChecksum(long lsn, int transactionId, long prevLSN,
                                         LogType type, int pageId,
                                         byte[] oldValue, byte[] newValue) {
        int hash = 17;
        hash = 31 * hash + Long.hashCode(lsn);
        hash = 31 * hash + Integer.hashCode(transactionId);
        hash = 31 * hash + Long.hashCode(prevLSN);
        hash = 31 * hash + type.hashCode();
        hash = 31 * hash + Integer.hashCode(pageId);
        hash = 31 * hash + Arrays.hashCode(oldValue);
        hash = 31 * hash + Arrays.hashCode(newValue);
        return hash;
    }

    // ==================== Getters ====================

    public long getLsn() {
        return lsn;
    }

    public int getTransactionId() {
        return transactionId;
    }

    public long getPrevLSN() {
        return prevLSN;
    }

    public LogType getType() {
        return type;
    }

    public int getPageId() {
        return pageId;
    }

    public byte[] getOldValue() {
        return oldValue;
    }

    public byte[] getNewValue() {
        return newValue;
    }

    public int getChecksum() {
        return checksum;
    }

    /**
     * Calculate the size of this log record in bytes.
     * Useful for buffer management and WAL file operations.
     */
    public int getSize() {
        int oldLen = (oldValue != null) ? oldValue.length : 0;
        int newLen = (newValue != null) ? newValue.length : 0;
        return HEADER_SIZE + oldLen + newLen;
    }

    // ==================== Utility ====================

    @Override
    public String toString() {
        return String.format(
                "LogRecord{lsn=%d, txn=%d, prevLSN=%d, type=%s, page=%d, " +
                        "oldSize=%d, newSize=%d, checksum=%08X}",
                lsn, transactionId, prevLSN, type, pageId,
                oldValue != null ? oldValue.length : 0,
                newValue != null ? newValue.length : 0,
                checksum
        );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof LogRecord)) return false;
        LogRecord that = (LogRecord) o;
        return lsn == that.lsn; // LSN is unique identifier
    }

    @Override
    public int hashCode() {
        return Long.hashCode(lsn);
    }
}


