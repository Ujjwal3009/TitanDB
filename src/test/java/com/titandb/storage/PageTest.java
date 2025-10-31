package com.titandb.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Page Tests")
public class PageTest {

    @Test
    @DisplayName("Create a new page")
    void testCreatePage() {
        Page page = new Page();

        assertNotNull(page);
        assertEquals(-1, page.getPageId());  // ← BACK TO -1!
        assertFalse(page.isDirty());
        assertFalse(page.isPinned());
        assertEquals(-1, page.getPageLSN());
    }

    @Test
    @DisplayName("Set and get page ID")
    void testSetGetPageId() {
        Page page = new Page();

        page.setPageId(42);

        assertEquals(42, page.getPageId());
        assertTrue(page.isDirty());
    }

    @Test
    @DisplayName("Set and get page type")
    void testSetGetPageType() {
        Page page = new Page();

        page.setPageType(Page.PageType.LEAF);

        assertEquals(Page.PageType.LEAF, page.getPageType());
        assertTrue(page.isDirty());
    }

    @Test
    @DisplayName("Pin and unpin page")
    void testPinUnpin() {
        Page page = new Page();

        assertFalse(page.isPinned());
        assertEquals(0, page.getPinCount());

        page.pin();
        assertTrue(page.isPinned());
        assertEquals(1, page.getPinCount());

        page.pin();
        assertEquals(2, page.getPinCount());

        page.unpin();
        assertEquals(1, page.getPinCount());

        page.unpin();
        assertFalse(page.isPinned());
        assertEquals(0, page.getPinCount());
    }

    @Test
    @DisplayName("Mark and clear dirty flag")
    void testDirtyFlag() {
        Page page = new Page();

        assertFalse(page.isDirty());

        page.markDirty();
        assertTrue(page.isDirty());

        page.clearDirty();
        assertFalse(page.isDirty());
    }

    @Test
    @DisplayName("Access data buffer")
    void testDataBuffer() {
        Page page = new Page();
        page.setPageId(1);

        ByteBuffer buffer = page.getDataBuffer();

        assertEquals(Page.HEADER_SIZE, buffer.position());
        assertEquals(Page.DATA_SIZE, buffer.remaining());

        buffer.putInt(12345);

        ByteBuffer readBuffer = page.getDataBuffer();
        assertEquals(12345, readBuffer.getInt());
    }

    @Test
    @DisplayName("Page size constants")
    void testPageSizeConstants() {
        assertEquals(4096, Page.PAGE_SIZE);
        assertEquals(16, Page.HEADER_SIZE);
        assertEquals(4080, Page.DATA_SIZE);
    }

    @Test
    @DisplayName("Create page from existing data")
    void testCreateFromData() {
        Page page1 = new Page();
        page1.setPageId(100);
        page1.setPageType(Page.PageType.INTERNAL);

        byte[] data = page1.getData();

        Page page2 = new Page(data);
        assertEquals(100, page2.getPageId());
        assertEquals(Page.PageType.INTERNAL, page2.getPageType());
    }

    @Test
    @DisplayName("Reset page clears all data")
    void testResetPage() {
        Page page = new Page();
        page.setPageId(42);
        page.setPageType(Page.PageType.LEAF);
        page.setPageLSN(9999);
        page.pin();
        page.markDirty();

        page.reset();

        // ✅ FIXED: Expect 0, not 42!
        assertEquals(0, page.getPageId());
        assertEquals(-1, page.getPageLSN());
        assertFalse(page.isDirty());
        assertEquals(0, page.getPinCount());
    }

    // ========== WEEK 4 LSN TESTS ==========

    @Test
    @DisplayName("NEW: Set and get page LSN")
    void testPageLSN() {
        Page page = new Page();

        assertEquals(-1, page.getPageLSN());

        page.setPageLSN(1042);
        assertEquals(1042, page.getPageLSN());
        assertTrue(page.isDirty());

        page.clearDirty();
        page.setPageLSN(2084);
        assertEquals(2084, page.getPageLSN());
        assertTrue(page.isDirty());
    }

    @Test
    @DisplayName("NEW: LSN persists in page header")
    void testLSNPersistence() {
        Page page = new Page();
        page.setPageId(10);
        page.setPageType(Page.PageType.LEAF);
        page.setPageLSN(5000);

        byte[] data = page.getData();

        ByteBuffer buffer = ByteBuffer.wrap(data);
        int storedPageId = buffer.getInt(0);
        byte storedType = buffer.get(4);
        long storedLSN = buffer.getLong(5);

        assertEquals(10, storedPageId);
        assertEquals(Page.PageType.LEAF.getValue(), storedType);
        assertEquals(5000, storedLSN);

        Page loadedPage = new Page(data);

        assertEquals(10, loadedPage.getPageId());
        assertEquals(Page.PageType.LEAF, loadedPage.getPageType());
        assertEquals(5000, loadedPage.getPageLSN());
    }

    @Test
    @DisplayName("NEW: Multiple LSN updates")
    void testMultipleLSNUpdates() {
        Page page = new Page();

        for (int i = 1; i <= 100; i++) {
            page.setPageLSN(i * 1000L);
            assertEquals(i * 1000L, page.getPageLSN());
        }

        assertEquals(100_000L, page.getPageLSN());
    }

    @Test
    @DisplayName("NEW: LSN in toString")
    void testToStringWithLSN() {
        Page page = new Page();
        page.setPageId(42);
        page.setPageLSN(9999);

        String str = page.toString();

        assertTrue(str.contains("id=42"));
        assertTrue(str.contains("lsn=9999"));
    }
}
