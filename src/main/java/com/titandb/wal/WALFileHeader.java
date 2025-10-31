package com.titandb.wal;

import java.nio.ByteBuffer;

/**
 * Header written at the beginning of each WAL segment file.
 *
 * Layout (64 bytes):
 * ┌────────────────────────────────┐
 * │ Magic Number (4 bytes)         │  0xDEADBEEF
 * ├────────────────────────────────┤
 * │ Version (4 bytes)              │  1
 * ├────────────────────────────────┤
 * │ Start LSN (8 bytes)            │  First LSN in this segment
 * ├────────────────────────────────┤
 * │ Reserved (48 bytes)            │  Future use
 * └────────────────────────────────┘
 */
public class WALFileHeader {

    public static final int HEADER_SIZE = 64;

    private final int magicNumber;
    private final int version;
    private final long startLSN;

    public WALFileHeader(int magicNumber, int version, long startLSN) {
        this.magicNumber = magicNumber;
        this.version = version;
        this.startLSN = startLSN;
    }

    /**
     * Serialize header to bytes.
     */
    public byte[] serialize() {
        ByteBuffer buffer = ByteBuffer.allocate(HEADER_SIZE);
        buffer.putInt(magicNumber);
        buffer.putInt(version);
        buffer.putLong(startLSN);
        // Rest is zeros (reserved)
        return buffer.array();
    }

    /**
     * Deserialize header from bytes.
     */
    public static WALFileHeader deserialize(byte[] bytes) {
        if (bytes.length < HEADER_SIZE) {
            throw new IllegalArgumentException("Invalid header size");
        }

        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        int magic = buffer.getInt();
        int version = buffer.getInt();
        long startLSN = buffer.getLong();

        return new WALFileHeader(magic, version, startLSN);
    }

    /**
     * Validate that this is a legitimate WAL file.
     */
    public boolean isValid() {
        return magicNumber == WALConfig.getMagicNumber() &&
                version == WALConfig.getVersion();
    }

    public int getMagicNumber() {
        return magicNumber;
    }

    public int getVersion() {
        return version;
    }

    public long getStartLSN() {
        return startLSN;
    }
}
