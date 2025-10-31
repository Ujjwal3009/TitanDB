package com.titandb.wal;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Manages the Write-Ahead Log (WAL) for the database.
 *
 * Responsibilities:
 * 1. Append log records to WAL file (append-only)
 * 2. Flush log buffer to disk (force-log-at-commit)
 * 3. Read log records during recovery
 * 4. Manage WAL segment files
 *
 * Thread-safety: Uses a lock to protect concurrent appends.
 */
public class LogManager {

    private final WALConfig config;
    private final LSNManager lsnManager;
    private final ByteBuffer logBuffer;
    private RandomAccessFile currentSegmentFile;
    private FileChannel currentSegment;
    private long currentSegmentNumber;
    private long segmentOffset;
    private final ReentrantLock appendLock;
    private volatile long flushedLSN;

    /**
     * Create a new LogManager.
     *
     * @param config WAL configuration
     * @param lsnManager LSN generator
     * @throws IOException if WAL directory cannot be created
     */
    public LogManager(WALConfig config, LSNManager lsnManager) throws IOException {
        this.config = config;
        this.lsnManager = lsnManager;
        this.logBuffer = ByteBuffer.allocate(config.getLogBufferSize());
        this.appendLock = new ReentrantLock();
        this.flushedLSN = -1;

        Files.createDirectories(config.getWalDirectory());

        this.currentSegmentNumber = 0;
        openSegment(currentSegmentNumber);
    }

    /**
     * Append a log record to the WAL.
     *
     * @param record The log record to append
     * @param force If true, immediately flush to disk (for COMMIT)
     * @return The LSN of the appended record
     * @throws IOException if write fails
     */
    public long appendLogRecord(LogRecord record, boolean force) throws IOException {
        byte[] recordBytes = record.serialize();

        appendLock.lock();
        try {
            // Check if we need to rotate to next segment
            if (segmentOffset + 4 + recordBytes.length > config.getSegmentSize()) {
                flush();
                rotateSegment();
            }

            // Check if buffer needs flushing
            if (logBuffer.remaining() < 4 + recordBytes.length) {
                flush();
            }

            // Write length prefix + record to buffer
            logBuffer.putInt(recordBytes.length);
            logBuffer.put(recordBytes);

            segmentOffset += 4 + recordBytes.length;

            if (force) {
                flush();
            }

            return record.getLsn();

        } finally {
            appendLock.unlock();
        }
    }

    /**
     * Flush the log buffer to disk.
     *
     * This is THE critical method for durability.
     * Calls fsync() to ensure data reaches physical disk.
     *
     * @throws IOException if flush fails
     */
    public void flush() throws IOException {
        appendLock.lock();
        try {
            if (logBuffer.position() == 0) {
                return; // Nothing to flush
            }

            // Flip to read mode
            logBuffer.flip();

            // Write all buffered data
            while (logBuffer.hasRemaining()) {
                currentSegment.write(logBuffer);
            }

            // CRITICAL: Force to physical disk (fsync)
            currentSegment.force(true);

            // Update flushed LSN
            flushedLSN = lsnManager.getCurrentLSN();

            // Clear buffer
            logBuffer.clear();

        } finally {
            appendLock.unlock();
        }
    }

    /**
     * Read all log records from WAL (used during recovery).
     *
     * @return List of all log records in LSN order
     * @throws IOException if read fails
     */
    public List<LogRecord> readAllLogRecords() throws IOException {
        List<LogRecord> records = new ArrayList<>();

        // Flush pending records
        flush();

        // Read all segments
        long segmentNum = 0;
        while (true) {
            Path segmentPath = getSegmentPath(segmentNum);
            if (!Files.exists(segmentPath)) {
                break;
            }

            records.addAll(readSegment(segmentPath));
            segmentNum++;
        }

        return records;
    }

    /**
     * Read all log records from a specific segment file.
     */
    private List<LogRecord> readSegment(Path segmentPath) throws IOException {
        List<LogRecord> records = new ArrayList<>();

        try (FileInputStream fis = new FileInputStream(segmentPath.toFile());
             DataInputStream dis = new DataInputStream(fis)) {

            // Skip header (64 bytes)
            dis.skipBytes(64);

            // Read records until EOF
            while (dis.available() > 0) {
                try {
                    // Read length
                    int recordLength = dis.readInt();

                    if (recordLength <= 0 || recordLength > 10 * 1024 * 1024) {
                        break; // Invalid or end of valid data
                    }

                    // Read record bytes
                    byte[] recordBytes = new byte[recordLength];
                    int bytesRead = dis.read(recordBytes);

                    if (bytesRead != recordLength) {
                        break; // Incomplete record
                    }

                    // Deserialize
                    LogRecord record = LogRecord.deserialize(recordBytes);
                    records.add(record);

                } catch (EOFException e) {
                    break;
                } catch (IllegalArgumentException e) {
                    // Checksum mismatch - corrupted record
                    break;
                }
            }
        }

        return records;
    }

    /**
     * Get the LSN that's guaranteed to be on disk.
     */
    public long getFlushedLSN() {
        return flushedLSN;
    }

    /**
     * Close the log manager and flush pending records.
     */
    public void close() throws IOException {
        appendLock.lock();
        try {
            flush();
            if (currentSegment != null) {
                currentSegment.close();
            }
            if (currentSegmentFile != null) {
                currentSegmentFile.close();
            }
        } finally {
            appendLock.unlock();
        }
    }

    /**
     * Open a new WAL segment file.
     */
    private void openSegment(long segmentNumber) throws IOException {
        Path segmentPath = getSegmentPath(segmentNumber);
        boolean isNewFile = !Files.exists(segmentPath);

        // Open file in read-write mode
        currentSegmentFile = new RandomAccessFile(segmentPath.toFile(), "rw");
        currentSegment = currentSegmentFile.getChannel();

        if (isNewFile) {
            // Write 64-byte header
            byte[] headerBytes = new byte[64];
            ByteBuffer header = ByteBuffer.wrap(headerBytes);

            header.putInt(WALConfig.getMagicNumber());
            header.putInt(WALConfig.getVersion());
            header.putLong(lsnManager.getCurrentLSN());
            // Remaining bytes are zeros (padding)

            header.position(0);
            header.limit(64);

            int written = currentSegment.write(header);
            if (written != 64) {
                throw new IOException("Failed to write complete header");
            }

            currentSegment.force(true);
            segmentOffset = 64;
        } else {
            // Existing file - seek to end
            segmentOffset = currentSegment.size();
            currentSegment.position(segmentOffset);
        }
    }

    /**
     * Rotate to the next WAL segment.
     */
    private void rotateSegment() throws IOException {
        currentSegment.force(true);
        currentSegment.close();
        currentSegmentFile.close();

        currentSegmentNumber++;
        openSegment(currentSegmentNumber);
    }

    /**
     * Get the file path for a segment number.
     */
    private Path getSegmentPath(long segmentNumber) {
        String fileName = config.getSegmentFileName(segmentNumber);
        return config.getWalDirectory().resolve(fileName);
    }
}
