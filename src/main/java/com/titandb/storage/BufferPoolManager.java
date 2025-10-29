package com.titandb.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Buffer Pool Manager - In-memory cache for disk pages.
 *
 * Responsibilities:
 * - Keep hot pages in RAM
 * - Fetch pages from disk on cache miss
 * - Evict cold pages when full (LRU policy)
 * - Write dirty pages back to disk
 *
 * Size: Fixed number of frames (e.g., 1000 frames = 4MB)
 *
 * Performance:
 * - Cache hit: ~1 μs (read from RAM)
 * - Cache miss: ~280 μs (read from disk)
 * - Target hit rate: 85-95%
 *
 * @author Ujjwal
 */
public class BufferPoolManager {
    private static final Logger logger = LoggerFactory.getLogger(BufferPoolManager.class);

    /** Number of frames in the pool */
    private final int poolSize;

    /** Array of frames (the actual cache) */
    private final Frame[] frames;

    /** Page table: PageId → FrameId */
    private final Map<Integer, Integer> pageTable;

    /** Disk manager for fetching pages */
    private final DiskManager diskManager;

    /** Statistics */
    private long cacheHits = 0;
    private long cacheMisses = 0;

    /**
     * Create a buffer pool.
     *
     * @param poolSize Number of frames (pages to cache)
     * @param diskManager Disk manager for I/O
     */
    public BufferPoolManager(int poolSize, DiskManager diskManager) {
        this.poolSize = poolSize;
        this.frames = new Frame[poolSize];
        this.pageTable = new HashMap<>();
        this.diskManager = diskManager;

        // Initialize all frames
        for (int i = 0; i < poolSize; i++) {
            frames[i] = new Frame();
        }

        logger.info("Created BufferPool: {} frames ({} KB)",
                poolSize, (poolSize * Page.PAGE_SIZE) / 1024);
    }

    /**
     * Fetch a page (from cache or disk).
     *
     * @param pageId Page ID to fetch
     * @return Page object
     */
    public synchronized Page fetchPage(int pageId) throws IOException {
        // Step 1: Check if page is already in cache
        if (pageTable.containsKey(pageId)) {
            int frameId = pageTable.get(pageId);
            Frame frame = frames[frameId];

            cacheHits++;
            logger.debug("Cache HIT: page {} in frame {}", pageId, frameId);

            frame.pin();  // Mark as in use
            return frame.getPage();
        }

        // Step 2: Cache miss - need to load from disk
        cacheMisses++;
        logger.debug("Cache MISS: page {} (hit rate: {:.2f}%)",
                pageId, getCacheHitRate() * 100);

        // Step 3: Find a free frame (or evict one)
        int frameId = findFreeFrame();
        if (frameId == -1) {
            frameId = evictFrame();
        }

        // Step 4: Load page from disk
        Page page = diskManager.readPage(pageId);

        // Step 5: Store in frame
        Frame frame = frames[frameId];
        frame.loadPage(page, pageId);
        frame.pin();  // Pin it immediately

        // Step 6: Update page table
        pageTable.put(pageId, frameId);

        logger.debug("Loaded page {} into frame {}", pageId, frameId);

        return page;
    }

    /**
     * Release a page (unpin it).
     */
    public synchronized void unpinPage(int pageId, boolean isDirty) {
        if (!pageTable.containsKey(pageId)) {
            logger.warn("Tried to unpin page {} not in buffer pool", pageId);
            return;
        }

        int frameId = pageTable.get(pageId);
        Frame frame = frames[frameId];

        frame.unpin();
        if (isDirty) {
            frame.markDirty();
        }

        logger.debug("Unpinned page {} (pinCount={})", pageId, frame.isPinned() ? "still pinned" : "0");
    }

    /**
     * Find a free frame.
     *
     * @return Frame ID or -1 if none free
     */
    private int findFreeFrame() {
        for (int i = 0; i < poolSize; i++) {
            if (frames[i].isEmpty()) {
                return i;
            }
        }
        return -1;  // No free frames
    }

    /**
     * Evict a frame using LRU policy.
     *
     * @return Frame ID that was evicted
     */
    private int evictFrame() throws IOException {
        // Simple LRU: Find oldest unpinned page
        int victimFrameId = -1;
        long oldestTime = Long.MAX_VALUE;

        for (int i = 0; i < poolSize; i++) {
            Frame frame = frames[i];

            // Skip pinned pages (in use)
            if (frame.isPinned()) {
                continue;
            }

            // Find least recently used
            if (frame.getLastAccessTime() < oldestTime) {
                oldestTime = frame.getLastAccessTime();
                victimFrameId = i;
            }
        }

        if (victimFrameId == -1) {
            throw new IOException("All pages are pinned! Cannot evict.");
        }

        Frame victim = frames[victimFrameId];
        int evictedPageId = victim.getPageId();

        // Write back if dirty
        if (victim.isDirty()) {
            diskManager.writePage(evictedPageId, victim.getPage());
            logger.debug("Wrote back dirty page {}", evictedPageId);
        }

        // Remove from page table
        pageTable.remove(evictedPageId);

        // Clear frame
        victim.clear();

        logger.debug("Evicted page {} from frame {}", evictedPageId, victimFrameId);

        return victimFrameId;
    }

    /**
     * Flush all dirty pages to disk.
     */
    public synchronized void flushAll() throws IOException {
        logger.info("Flushing all dirty pages...");

        int flushedCount = 0;
        for (Frame frame : frames) {
            if (!frame.isEmpty() && frame.isDirty()) {
                diskManager.writePage(frame.getPageId(), frame.getPage());
                flushedCount++;
            }
        }

        logger.info("Flushed {} dirty pages", flushedCount);
    }

    /**
     * Get cache hit rate (0.0 to 1.0).
     */
    public double getCacheHitRate() {
        long total = cacheHits + cacheMisses;
        return total == 0 ? 0.0 : (double) cacheHits / total;
    }

    /**
     * Get statistics.
     */
    public String getStatistics() {
        return String.format(
                "BufferPool[size=%d, hits=%d, misses=%d, hitRate=%.2f%%]",
                poolSize, cacheHits, cacheMisses, getCacheHitRate() * 100
        );
    }

    @Override
    public String toString() {
        return getStatistics();
    }
}
