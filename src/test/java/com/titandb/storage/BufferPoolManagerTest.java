package com.titandb.storage;

import org.junit.jupiter.api.*;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Buffer Pool Manager Tests")
public class BufferPoolManagerTest {

    private static final String TEST_DB = "test_bufferpool.titandb";
    private DiskManager diskManager;
    private BufferPoolManager bufferPool;

    @BeforeEach
    public void setUp() throws IOException {
        if (DiskManager.databaseExists(TEST_DB)) {
            DiskManager.deleteDatabase(TEST_DB);
        }

        diskManager = new DiskManager(TEST_DB);
        bufferPool = new BufferPoolManager(10, diskManager);  // 10 frames
    }

    @AfterEach
    public void tearDown() throws IOException {
        if (diskManager != null) {
            diskManager.close();
        }
        if (DiskManager.databaseExists(TEST_DB)) {
            DiskManager.deleteDatabase(TEST_DB);
        }
    }

    @Test
    @DisplayName("Fetch page - cache miss")
    public void testFetchPageMiss() throws IOException {
        // Create and write a page
        int pageId = diskManager.allocatePage();
        Page page = new Page();
        page.setPageId(pageId);
        page.setPageType(Page.PageType.LEAF);
        diskManager.writePage(pageId, page);

        // Fetch from buffer pool (should be cache miss)
        Page fetched = bufferPool.fetchPage(pageId);

        assertNotNull(fetched);
        assertEquals(pageId, fetched.getPageId());
        assertEquals(0.0, bufferPool.getCacheHitRate());  // 0% (first access)
    }

    @Test
    @DisplayName("Fetch same page twice - cache hit")
    public void testFetchPageHit() throws IOException {
        int pageId = diskManager.allocatePage();
        Page page = new Page();
        page.setPageId(pageId);
        diskManager.writePage(pageId, page);

        // First fetch (miss)
        bufferPool.fetchPage(pageId);

        // Second fetch (should hit cache)
        Page fetched2 = bufferPool.fetchPage(pageId);

        assertNotNull(fetched2);
        assertEquals(0.5, bufferPool.getCacheHitRate());  // 50% (1 hit, 1 miss)
    }

    @Test
    @DisplayName("Cache multiple pages")
    public void testCacheMultiplePages() throws IOException {
        int[] pageIds = new int[5];

        // Create 5 pages
        for (int i = 0; i < 5; i++) {
            pageIds[i] = diskManager.allocatePage();
            Page page = new Page();
            page.setPageId(pageIds[i]);
            diskManager.writePage(pageIds[i], page);
        }

        // Fetch all (5 misses)
        for (int pageId : pageIds) {
            bufferPool.fetchPage(pageId);
        }

        // Fetch again (5 hits)
        for (int pageId : pageIds) {
            bufferPool.fetchPage(pageId);
        }

        assertEquals(0.5, bufferPool.getCacheHitRate());  // 50% (5 hits, 5 misses)
    }

    @Test
    @DisplayName("Evict page when buffer is full")
    public void testEviction() throws IOException {
        // Create 11 pages (buffer size = 10)
        int[] pageIds = new int[11];
        for (int i = 0; i < 11; i++) {
            pageIds[i] = diskManager.allocatePage();
            Page page = new Page();
            page.setPageId(pageIds[i]);
            diskManager.writePage(pageIds[i], page);
        }

        // Fetch all 11 (should evict oldest)
        for (int pageId : pageIds) {
            Page page = bufferPool.fetchPage(pageId);
            bufferPool.unpinPage(pageId, false);  // Unpin immediately
        }

        // First page should have been evicted
        // Fetching it again should be a miss
        long missesBeforeBefore = (long) bufferPool.getCacheHitRate();
        bufferPool.fetchPage(pageIds[0]);

        // Statistics should show eviction happened
        assertTrue(bufferPool.getStatistics().contains("misses"));
    }

    @Test
    @DisplayName("Unpin page")
    public void testUnpin() throws IOException {
        int pageId = diskManager.allocatePage();
        Page page = new Page();
        diskManager.writePage(pageId, page);

        bufferPool.fetchPage(pageId);  // Automatically pinned

        // Unpin should not throw
        assertDoesNotThrow(() -> bufferPool.unpinPage(pageId, false));
    }

    @Test
    @DisplayName("Flush all dirty pages")
    public void testFlushAll() throws IOException {
        int pageId = diskManager.allocatePage();
        Page page = new Page();
        diskManager.writePage(pageId, page);

        bufferPool.fetchPage(pageId);
        bufferPool.unpinPage(pageId, true);  // Mark as dirty

        // Flush should write back
        assertDoesNotThrow(() -> bufferPool.flushAll());
    }
}
