package com.titandb.storage;

import java.util.Objects;

/**
 * Unique identifier for a page in the database.
 *
 * In a simple implementation, this is just an integer.
 * In production databases, it might include:
 * - File ID (for multi-file databases)
 * - Segment ID (for partitioning)
 * - Page number within file
 *
 * For TitanDB, we keep it simple: one file, sequential page IDs.
 */
public class PageId {
    private final int pageNum;

    public PageId(int pageNum) {
        if (pageNum < 0) {
            throw new IllegalArgumentException("Page number must be non-negative");
        }
        this.pageNum = pageNum;
    }

    public int getPageNum() {
        return pageNum;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PageId)) return false;
        PageId pageId = (PageId) o;
        return pageNum == pageId.pageNum;
    }

    @Override
    public int hashCode() {
        return Objects.hash(pageNum);
    }

    @Override
    public String toString() {
        return "PageId(" + pageNum + ")";
    }

    /**
     * Invalid page ID (e.g., for null references).
     */
    public static final PageId INVALID = new PageId(-1);
}
