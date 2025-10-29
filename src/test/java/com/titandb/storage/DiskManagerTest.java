package com.titandb.storage;

import org.junit.jupiter.api.*;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("DiskManager Tests")
public class DiskManagerTest {

    private static final String TEST_DB = "test_diskmanager.titandb";
    private DiskManager diskManager;

    @BeforeEach
    public void setUp() throws IOException {
        // Delete test database if exists
        if (DiskManager.databaseExists(TEST_DB)) {
            DiskManager.deleteDatabase(TEST_DB);
        }

        diskManager = new DiskManager(TEST_DB);
    }

    @AfterEach
    public void tearDown() throws IOException {
        if (diskManager != null) {
            diskManager.close();
        }

        // Clean up test file
        if (DiskManager.databaseExists(TEST_DB)) {
            DiskManager.deleteDatabase(TEST_DB);
        }
    }

    @Test
    @DisplayName("Create new database file")
    public void testCreateDatabase() throws IOException {
        assertTrue(DiskManager.databaseExists(TEST_DB));
        assertEquals(1, diskManager.getNumPages()); // Header page
        assertTrue(diskManager.getFileSize() >= Page.PAGE_SIZE);
    }

    @Test
    @DisplayName("Read header page")
    public void testReadHeaderPage() throws IOException {
        Page headerPage = diskManager.readPage(0);

        assertEquals(0, headerPage.getPageId());
        assertEquals(Page.PageType.HEADER, headerPage.getPageType());
    }

    @Test
    @DisplayName("Allocate new pages")
    public void testAllocatePages() {
        int page1 = diskManager.allocatePage();
        int page2 = diskManager.allocatePage();
        int page3 = diskManager.allocatePage();

        assertEquals(1, page1);
        assertEquals(2, page2);
        assertEquals(3, page3);
        assertEquals(4, diskManager.getNumPages()); // 0=header, 1,2,3=allocated
    }

    @Test
    @DisplayName("Write and read a page")
    public void testWriteAndReadPage() throws IOException {
        // Create a page with data
        Page originalPage = new Page();
        originalPage.setPageId(1);
        originalPage.setPageType(Page.PageType.LEAF);

        ByteBuffer buffer = originalPage.getDataBuffer();
        buffer.putInt(42);
        buffer.put("TitanDB".getBytes());

        // Write to disk
        diskManager.writePage(1, originalPage);

        // Read back
        Page loadedPage = diskManager.readPage(1);

        assertEquals(1, loadedPage.getPageId());
        assertEquals(Page.PageType.LEAF, loadedPage.getPageType());

        ByteBuffer loadedBuffer = loadedPage.getDataBuffer();
        assertEquals(42, loadedBuffer.getInt());

        byte[] strBytes = new byte[7];
        loadedBuffer.get(strBytes);
        assertEquals("TitanDB", new String(strBytes));
    }

    @Test
    @DisplayName("Write multiple pages")
    public void testWriteMultiplePages() throws IOException {
        for (int i = 1; i <= 10; i++) {
            Page page = new Page();
            page.setPageId(i);
            page.setPageType(Page.PageType.LEAF);

            ByteBuffer buffer = page.getDataBuffer();
            buffer.putInt(i * 100);

            diskManager.writePage(i, page);
        }

        assertEquals(11, diskManager.getNumPages()); // 0=header, 1-10=data

        // Verify all pages
        for (int i = 1; i <= 10; i++) {
            Page page = diskManager.readPage(i);
            assertEquals(i, page.getPageId());

            ByteBuffer buffer = page.getDataBuffer();
            assertEquals(i * 100, buffer.getInt());
        }
    }

    @Test
    @DisplayName("Page persists across DiskManager instances")
    public void testPersistenceAcrossInstances() throws IOException {
        // Write a page
        Page page = new Page();
        page.setPageId(1);
        page.setPageType(Page.PageType.LEAF);

        ByteBuffer buffer = page.getDataBuffer();
        buffer.putInt(999);
        diskManager.writePage(1, page);

        // Close DiskManager
        diskManager.close();

        // Reopen database
        diskManager = new DiskManager(TEST_DB);

        // Read page back
        Page loadedPage = diskManager.readPage(1);
        ByteBuffer loadedBuffer = loadedPage.getDataBuffer();

        assertEquals(999, loadedBuffer.getInt());
    }

    @Test
    @DisplayName("Read invalid page ID throws exception")
    public void testReadInvalidPageId() {
        assertThrows(IllegalArgumentException.class, () -> {
            diskManager.readPage(-1);
        });

        assertThrows(IllegalArgumentException.class, () -> {
            diskManager.readPage(999); // Doesn't exist
        });
    }

    @Test
    @DisplayName("Write clears dirty flag")
    public void testWriteClearsDirtyFlag() throws IOException {
        Page page = new Page();
        page.setPageId(1);
        page.markDirty();
        assertTrue(page.isDirty());

        diskManager.writePage(1, page);
        assertFalse(page.isDirty());
    }

    @Test
    @DisplayName("Read and write statistics")
    public void testStatistics() throws IOException {
        assertEquals(0, diskManager.getReadCount());
        assertEquals(1, diskManager.getWriteCount()); // Header page

        diskManager.readPage(0);
        assertEquals(1, diskManager.getReadCount());

        Page page = new Page();
        diskManager.writePage(1, page);
        assertEquals(2, diskManager.getWriteCount());
    }

    @Test
    @DisplayName("Flush writes data to disk")
    public void testFlush() throws IOException {
        Page page = new Page();
        page.setPageId(1);
        diskManager.writePage(1, page);

        assertDoesNotThrow(() -> diskManager.flush());
    }

    @Test
    @DisplayName("Large sequential write and read")
    public void testLargeSequentialIO() throws IOException {
        int numPages = 100;

        // Write 100 pages
        for (int i = 1; i <= numPages; i++) {
            Page page = new Page();
            page.setPageId(i);
            page.setPageType(Page.PageType.LEAF);

            ByteBuffer buffer = page.getDataBuffer();
            buffer.putInt(i);
            buffer.putLong(i * 1000L);

            diskManager.writePage(i, page);
        }

        // Read and verify
        for (int i = 1; i <= numPages; i++) {
            Page page = diskManager.readPage(i);
            ByteBuffer buffer = page.getDataBuffer();

            assertEquals(i, buffer.getInt());
            assertEquals(i * 1000L, buffer.getLong());
        }
    }
}
