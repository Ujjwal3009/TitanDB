package com.titandb.storage;

/**
 * A Frame is a slot in the buffer pool that holds one page.
 *
 * Analogy: Frame = parking spot, Page = car
 *
 * Frame tracks:
 * - Which page is stored here (page ID)
 * - Is the page modified? (dirty flag)
 * - How many threads are using it? (pin count)
 * - When was it last used? (for LRU)
 *
 * @author Ujjwal
 */
public class Frame {
    /** The actual page data in memory */
    private Page page;

    /** Which page ID is stored here (-1 = empty) */
    private int pageId;

    /** Is this page modified? (needs write-back) */
    private boolean dirty;

    /** How many threads are using this page? */
    private int pinCount;

    /** Timestamp of last access (for LRU) */
    private long lastAccessTime;

    public Frame() {
        this.page = null;
        this.pageId = -1;
        this.dirty = false;
        this.pinCount = 0;
        this.lastAccessTime = 0;
    }

    /**
     * Load a page into this frame.
     */
    public void loadPage(Page page, int pageId) {
        this.page = page;
        this.pageId = pageId;
        this.dirty = false;
        this.pinCount = 0;
        this.lastAccessTime = System.nanoTime();
    }

    /**
     * Get the page stored in this frame.
     */
    public Page getPage() {
        this.lastAccessTime = System.nanoTime();
        return page;
    }

    /**
     * Get the page ID stored here.
     */
    public int getPageId() {
        return pageId;
    }

    /**
     * Check if this frame is empty.
     */
    public boolean isEmpty() {
        return pageId == -1;
    }

    /**
     * Mark this page as modified.
     */
    public void markDirty() {
        this.dirty = true;
    }

    /**
     * Check if page is dirty.
     */
    public boolean isDirty() {
        return dirty;
    }

    /**
     * Increment pin count (page in use).
     */
    public void pin() {
        pinCount++;
    }

    /**
     * Decrement pin count (page released).
     */
    public void unpin() {
        if (pinCount > 0) {
            pinCount--;
        }
    }

    /**
     * Check if page is pinned.
     */
    public boolean isPinned() {
        return pinCount > 0;
    }

    /**
     * Get last access timestamp.
     */
    public long getLastAccessTime() {
        return lastAccessTime;
    }

    /**
     * Clear this frame (evict page).
     */
    public void clear() {
        this.page = null;
        this.pageId = -1;
        this.dirty = false;
        this.pinCount = 0;
        this.lastAccessTime = 0;
    }

    @Override
    public String toString() {
        return String.format("Frame[pageId=%d, dirty=%b, pinned=%d]",
                pageId, dirty, pinCount);
    }
}
