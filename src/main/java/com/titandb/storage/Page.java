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
 * ┌──────────────────────────────────────┐
 * │ Page Header (16 bytes)               │
 * │  - Page ID (4 bytes)                 │
 * │  - Page Type (1 byte)                │
 * │  - LSN (8 bytes) [for WAL, Week 4]   │
 * │  - Checksum (2 bytes) [optional]     │
 * │  - Flags (1 byte)                    │
 * ├──────────────────────────────────────┤
 * │ Data (4080 bytes)                    │
 * │  - Actual node/record data           │
 * └──────────────────────────────────────┘
 *
 * @author Ujjwal
 */
public class Page {
    /** Standard page size: 4KB (matches OS page size) */
    public static final int PAGE_SIZE = 4096;

    /** Size of page header (metadata) */
    public static final int HEADER_SIZE = 16;

    /** Usable data size after header */
    public static final int DATA_SIZE = PAGE_SIZE - HEADER_SIZE;

    /** The actual page data */
    private final byte[] data;

    /** Unique page identifier */
    private int pageId;

    /** Has this page been modified since last flush? */
    private boolean dirty;

    /** How many threads are currently using this page? */
    private int pinCount;

    /** Page type enumeration */
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
                if (type.value == b) return type;
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
    }

    /**
     * Create a page from existing data (e.g., read from disk).
     */
    public Page(byte[] data) {
        if (data.length != PAGE_SIZE) {
            throw new IllegalArgumentException(
                    "Page data must be exactly " + PAGE_SIZE + " bytes"
            );
        }
        this.data = Arrays.copyOf(data, PAGE_SIZE);

        // Read page ID from header
        ByteBuffer buffer = ByteBuffer.wrap(this.data);
        this.pageId = buffer.getInt(0);

        this.dirty = false;
        this.pinCount = 0;
    }

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
        ByteBuffer buffer = ByteBuffer.wrap(data);
        buffer.putInt(0, pageId);
        markDirty();
    }

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

    /**
     * Get raw page data (for disk I/O).
     */
    public byte[] getData() {
        return data;
    }

    /**
     * Get a ByteBuffer view of the data section (skip header).
     */
    public ByteBuffer getDataBuffer() {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        buffer.position(HEADER_SIZE);
        return buffer;
    }

    /**
     * Check if page has been modified.
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
     */
    public boolean isPinned() {
        return pinCount > 0;
    }

    /**
     * Get current pin count.
     */
    public int getPinCount() {
        return pinCount;
    }

    /**
     * Reset page to blank state.
     */
    public void reset() {
        Arrays.fill(data, (byte) 0);
        dirty = false;
        pinCount = 0;
    }

    @Override
    public String toString() {
        return String.format(
                "Page[id=%d, type=%s, dirty=%b, pinned=%d]",
                pageId, getPageType(), dirty, pinCount
        );
    }
}
