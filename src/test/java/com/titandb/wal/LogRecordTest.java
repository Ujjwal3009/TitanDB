package com.titandb.wal;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive tests for LogRecord serialization and deserialization.
 */
@DisplayName("LogRecord Tests")
public class LogRecordTest {

    @Test
    @DisplayName("Create simple INSERT log record")
    public void testCreateInsertRecord() {
        byte[] value = "key=42,value=hello".getBytes();

        LogRecord record = LogRecord.builder()
                .lsn(1001)
                .transactionId(77)
                .prevLSN(1000)
                .type(LogType.INSERT)
                .pageId(10)
                .newValue(value)
                .build();

        assertEquals(1001, record.getLsn());
        assertEquals(77, record.getTransactionId());
        assertEquals(1000, record.getPrevLSN());
        assertEquals(LogType.INSERT, record.getType());
        assertEquals(10, record.getPageId());
        assertNull(record.getOldValue()); // INSERT has no old value
        assertArrayEquals(value, record.getNewValue());
    }

    @Test
    @DisplayName("Create UPDATE log record with old and new values")
    public void testCreateUpdateRecord() {
        byte[] oldVal = "balance=100".getBytes();
        byte[] newVal = "balance=150".getBytes();

        LogRecord record = LogRecord.builder()
                .lsn(2042)
                .transactionId(88)
                .prevLSN(2040)
                .type(LogType.UPDATE)
                .pageId(512)
                .oldValue(oldVal)
                .newValue(newVal)
                .build();

        assertEquals(LogType.UPDATE, record.getType());
        assertArrayEquals(oldVal, record.getOldValue());
        assertArrayEquals(newVal, record.getNewValue());
    }

    @Test
    @DisplayName("Create DELETE log record")
    public void testCreateDeleteRecord() {
        byte[] oldVal = "key=42,value=deleted".getBytes();

        LogRecord record = LogRecord.builder()
                .lsn(3050)
                .transactionId(99)
                .prevLSN(3045)
                .type(LogType.DELETE)
                .pageId(20)
                .oldValue(oldVal)
                .build();

        assertEquals(LogType.DELETE, record.getType());
        assertArrayEquals(oldVal, record.getOldValue());
        assertNull(record.getNewValue()); // DELETE has no new value
    }

    @Test
    @DisplayName("Serialize and deserialize INSERT record")
    public void testSerializeDeserializeInsert() {
        byte[] value = "test data".getBytes();

        LogRecord original = LogRecord.builder()
                .lsn(5001)
                .transactionId(10)
                .prevLSN(-1) // No previous (BEGIN)
                .type(LogType.INSERT)
                .pageId(100)
                .newValue(value)
                .build();

        // Serialize
        byte[] bytes = original.serialize();

        // Deserialize
        LogRecord reconstructed = LogRecord.deserialize(bytes);

        // Verify all fields match
        assertEquals(original.getLsn(), reconstructed.getLsn());
        assertEquals(original.getTransactionId(), reconstructed.getTransactionId());
        assertEquals(original.getPrevLSN(), reconstructed.getPrevLSN());
        assertEquals(original.getType(), reconstructed.getType());
        assertEquals(original.getPageId(), reconstructed.getPageId());
        assertArrayEquals(original.getNewValue(), reconstructed.getNewValue());
        assertEquals(original.getChecksum(), reconstructed.getChecksum());
    }

    @Test
    @DisplayName("Serialize and deserialize UPDATE record")
    public void testSerializeDeserializeUpdate() {
        byte[] oldVal = "old".getBytes();
        byte[] newVal = "new".getBytes();

        LogRecord original = LogRecord.builder()
                .lsn(6042)
                .transactionId(20)
                .prevLSN(6040)
                .type(LogType.UPDATE)
                .pageId(200)
                .oldValue(oldVal)
                .newValue(newVal)
                .build();

        byte[] bytes = original.serialize();
        LogRecord reconstructed = LogRecord.deserialize(bytes);

        assertArrayEquals(oldVal, reconstructed.getOldValue());
        assertArrayEquals(newVal, reconstructed.getNewValue());
    }

    @Test
    @DisplayName("Corrupted checksum throws exception")
    public void testCorruptedChecksum() {
        LogRecord original = LogRecord.builder()
                .lsn(7001)
                .transactionId(30)
                .type(LogType.INSERT)
                .pageId(300)
                .newValue("data".getBytes())
                .build();

        byte[] bytes = original.serialize();

        // Corrupt the checksum (last 4 bytes)
        bytes[bytes.length - 1] ^= 0xFF;

        // Should throw exception
        assertThrows(IllegalArgumentException.class, () -> {
            LogRecord.deserialize(bytes);
        });
    }

    @Test
    @DisplayName("LogRecord size calculation")
    public void testLogRecordSize() {
        byte[] value = new byte[100]; // 100 bytes

        LogRecord record = LogRecord.builder()
                .lsn(8001)
                .transactionId(40)
                .type(LogType.INSERT)
                .pageId(400)
                .newValue(value)
                .build();

        // Header = 37 bytes
        // New value = 100 bytes
        // Total = 137 bytes
        assertEquals(137, record.getSize());
        assertEquals(137, record.serialize().length);
    }

    @Test
    @DisplayName("BEGIN transaction record")
    public void testBeginRecord() {
        LogRecord record = LogRecord.builder()
                .lsn(9001)
                .transactionId(50)
                .prevLSN(-1) // No previous for BEGIN
                .type(LogType.BEGIN)
                .pageId(-1) // No page for transaction control
                .build();

        assertEquals(LogType.BEGIN, record.getType());
        assertEquals(-1, record.getPrevLSN());
        assertNull(record.getOldValue());
        assertNull(record.getNewValue());
    }

    @Test
    @DisplayName("COMMIT transaction record")
    public void testCommitRecord() {
        LogRecord record = LogRecord.builder()
                .lsn(9010)
                .transactionId(50)
                .prevLSN(9005) // Points to last operation
                .type(LogType.COMMIT)
                .pageId(-1)
                .build();

        assertEquals(LogType.COMMIT, record.getType());
        assertEquals(9005, record.getPrevLSN());
    }

    @Test
    @DisplayName("Large value serialization")
    public void testLargeValue() {
        // Simulate a large value (4KB)
        byte[] largeValue = new byte[4096];
        for (int i = 0; i < largeValue.length; i++) {
            largeValue[i] = (byte) (i % 256);
        }

        LogRecord record = LogRecord.builder()
                .lsn(10001)
                .transactionId(60)
                .type(LogType.INSERT)
                .pageId(500)
                .newValue(largeValue)
                .build();

        byte[] bytes = record.serialize();
        LogRecord reconstructed = LogRecord.deserialize(bytes);

        assertArrayEquals(largeValue, reconstructed.getNewValue());
    }
}
