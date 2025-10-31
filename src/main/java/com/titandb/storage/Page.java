package com.titandb.storage;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Represents a fixed-size page (4KB block) used for disk I/O.
 *
 * Pages are the unit of disk storage - we read/write entire pages,
 * never individual bytes. This matches OS page cache behavior.
 *
 * Page Layout:
 * ┌────────────────────────────────────────┐
 * │ Page Header (16 bytes)                 │
 * │  - Page ID (4 bytes)                   │
 * │  - Page Type (1 byte)                  │
 * │  - LSN (8 bytes) - for WAL (Week 4)    │
 * │  - Checksum (2 bytes) - optional       │
 * │  - Flags (1 byte)                      │
 * ├────────────────────────────────────────┤
 * │ Data (4080 bytes)                      │
 * │  - Actual node/record data             │
 * └────────────────────────────────────────┘
 *
 * @author Ujjwal
 */
public class Page {

    /**
     * Standard page size: 4KB (matches OS page size)
     */
    public static final int PAGE_SIZE = 4096;

    /**
     * Size of page header metadata
     */
    public static final int HEADER_SIZE = 16;

    /**
     * Usable data size after header
     */
    public static final int DATA_SIZE = PAGE_SIZE - HEADER_SIZE;

    /**
     * The actual page data (4096 bytes)
     */
    private final byte[] data;

    /**
     * Unique page identifier
     */
    private int pageId;

    /**
     * Has this page been modified since last flush?
     */
    private boolean dirty;

    /**
     * How many threads are currently using this page?
     * (Used by BufferPoolManager for eviction policy)
     */
    private int pinCount;

    /**
     * Log Sequence Number - last modification to this page.
     * Used during recovery to determine if a logged change was applied.
     * -1 means no modifications logged yet.
     *
     * NEW in Week 4!
     */
    private long pageLSN;

    /**
     * Page type enumeration
     */
    public enum PageType {
        INVALID(0),
        HEADER(1),      // Database metadata page
        INTERNAL(2),    // B+ tree internal node
        LEAF(3);        // B+ tree leaf node

        private final byte value;

        PageType(int value) {
            this.value = (byte) value;
        }

        public byte getValue() {
            return value;
        }

        public static PageType fromByte(byte b) {
            for (PageType type : values()) {
                if (type.value == b) {
                    return type;
                }
            }
            return INVALID;
        }
    }

    /**
     * Create a new blank page.
     */
    public Page() {
        this.data = new byte[PAGE_SIZE];
        this.pageId = -1;  // Invalid until assigned
        this.dirty = false;
        this.pinCount = 0;
        this.pageLSN = -1;  // No modifications yet
    }

    /**
     * Create a page from existing data (e.g., read from disk).
     *
     * @param data Existing page data (must be exactly 4096 bytes)
     */
    public Page(byte[] data) {
        if (data.length != PAGE_SIZE) {
            throw new IllegalArgumentException("Page data must be exactly " + PAGE_SIZE + " bytes");
        }

        this.data = Arrays.copyOf(data, PAGE_SIZE);

        // Read page ID from header
        ByteBuffer buffer = ByteBuffer.wrap(this.data);
        this.pageId = buffer.getInt(0);

        // Read LSN from header (NEW!)
        this.pageLSN = buffer.getLong(5);  // Offset 5 (after pageId and type)

        this.dirty = false;
        this.pinCount = 0;
    }

    // ==================== Page ID ====================

    /**
     * Get the page ID.
     */
    public int getPageId() {
        return pageId;
    }

    /**
     * Set the page ID and update header.
     */
    public void setPageId(int pageId) {
        this.pageId = pageId;

        // Write to header
        ByteBuffer buffer = ByteBuffer.wrap(data);
        buffer.putInt(0, pageId);

        markDirty();
    }

    // ==================== Page Type ====================

    /**
     * Get page type from header.
     */
    public PageType getPageType() {
        return PageType.fromByte(data[4]);
    }

    /**
     * Set page type in header.
     */
    public void setPageType(PageType type) {
        data[4] = type.getValue();
        markDirty();
    }

    // ==================== LSN (NEW in Week 4!) ====================

    /**
     * Get the page LSN.
     * Returns -1 if no modifications have been logged.
     *
     * @return Log Sequence Number of last modification
     */
    public long getPageLSN() {
        return pageLSN;
    }

    /**
     * Set the page LSN.
     * This is called when a logged operation modifies the page.
     *
     * @param lsn Log Sequence Number to set
     */
    public void setPageLSN(long lsn) {
        this.pageLSN = lsn;

        // Write to header (offset 5-12, 8 bytes)
        ByteBuffer buffer = ByteBuffer.wrap(data);
        buffer.putLong(5, lsn);

        markDirty();
    }

    // ==================== Data Access ====================

    /**
     * Get raw page data for disk I/O.
     * Returns the entire 4096-byte array.
     *
     * @return Full page data including header
     */
    public byte[] getData() {
        return data;
    }

    /**
     * Get a ByteBuffer view of the data section (skip header).
     * This is what applications use to read/write actual content.
     *
     * @return ByteBuffer positioned at start of data section
     */
    public ByteBuffer getDataBuffer() {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        buffer.position(HEADER_SIZE);
        return buffer;
    }

    // ==================== Dirty Flag ====================

    /**
     * Check if page has been modified.
     *
     * @return true if page needs to be written to disk
     */
    public boolean isDirty() {
        return dirty;
    }

    /**
     * Mark page as modified (needs flush to disk).
     */
    public void markDirty() {
        this.dirty = true;
    }

    /**
     * Clear dirty flag (after flushing to disk).
     */
    public void clearDirty() {
        this.dirty = false;
    }

    // ==================== Pin Count ====================

    /**
     * Increment pin count (page in use, don't evict).
     */
    public void pin() {
        pinCount++;
    }

    /**
     * Decrement pin count.
     */
    public void unpin() {
        if (pinCount > 0) {
            pinCount--;
        }
    }

    /**
     * Check if page is pinned (in use).
     *
     * @return true if pin count > 0
     */
    public boolean isPinned() {
        return pinCount > 0;
    }

    /**
     * Get current pin count.
     *
     * @return Number of threads using this page
     */
    public int getPinCount() {
        return pinCount;
    }

    // ==================== Utility ====================

    /**

     * Reset page to blank state.
     */
    public void reset() {
        // Clear all data bytes
        Arrays.fill(data, (byte) 0);

        // Reset fields
        pageId = 0;           // ← ADD THIS!
        dirty = false;
        pinCount = 0;
        pageLSN = -1;
    }


    @Override
    public String toString() {
        return String.format("Page{id=%d, type=%s, lsn=%d, dirty=%b, pinned=%d}",
                pageId, getPageType(), pageLSN, dirty, pinCount);
    }
}
