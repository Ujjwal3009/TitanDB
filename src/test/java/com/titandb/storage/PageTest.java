package com.titandb.storage;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Page Storage Tests")
public class PageTest {

    private Page page;

    @BeforeEach
    public void setUp() {
        page = new Page();
    }

    @Test
    @DisplayName("New page should have correct size")
    public void testPageSize() {
        assertEquals(Page.PAGE_SIZE, page.getData().length);
        assertEquals(4096, page.getData().length);
    }

    @Test
    @DisplayName("New page should not be dirty")
    public void testNewPageNotDirty() {
        assertFalse(page.isDirty());
    }

    @Test
    @DisplayName("New page should not be pinned")
    public void testNewPageNotPinned() {
        assertFalse(page.isPinned());
        assertEquals(0, page.getPinCount());
    }

    @Test
    @DisplayName("Set and get page ID")
    public void testPageId() {
        page.setPageId(42);
        assertEquals(42, page.getPageId());
        assertTrue(page.isDirty()); // Setting ID marks as dirty
    }

    @Test
    @DisplayName("Set and get page type")
    public void testPageType() {
        page.setPageType(Page.PageType.LEAF);
        assertEquals(Page.PageType.LEAF, page.getPageType());

        page.setPageType(Page.PageType.INTERNAL);
        assertEquals(Page.PageType.INTERNAL, page.getPageType());
    }

    @Test
    @DisplayName("Pin and unpin operations")
    public void testPinUnpin() {
        page.pin();
        assertTrue(page.isPinned());
        assertEquals(1, page.getPinCount());

        page.pin();
        assertEquals(2, page.getPinCount());

        page.unpin();
        assertEquals(1, page.getPinCount());
        assertTrue(page.isPinned());

        page.unpin();
        assertEquals(0, page.getPinCount());
        assertFalse(page.isPinned());
    }

    @Test
    @DisplayName("Mark dirty and clear dirty")
    public void testDirtyFlag() {
        assertFalse(page.isDirty());

        page.markDirty();
        assertTrue(page.isDirty());

        page.clearDirty();
        assertFalse(page.isDirty());
    }

    @Test
    @DisplayName("Write and read data from page")
    public void testDataReadWrite() {
        page.setPageId(10);
        page.setPageType(Page.PageType.LEAF);

        // Write some data
        ByteBuffer buffer = page.getDataBuffer();
        buffer.putInt(100);  // Write integer
        buffer.put("Hello".getBytes());  // Write string

        // Read it back
        ByteBuffer readBuffer = page.getDataBuffer();
        assertEquals(100, readBuffer.getInt());

        byte[] strBytes = new byte[5];
        readBuffer.get(strBytes);
        assertEquals("Hello", new String(strBytes));
    }

    @Test
    @DisplayName("Create page from existing data")
    public void testPageFromData() {
        // Create a page and set ID
        page.setPageId(99);
        page.setPageType(Page.PageType.INTERNAL);

        // Get raw data
        byte[] rawData = page.getData();

        // Create new page from raw data
        Page loadedPage = new Page(rawData);

        assertEquals(99, loadedPage.getPageId());
        assertEquals(Page.PageType.INTERNAL, loadedPage.getPageType());
    }

    @Test
    @DisplayName("Reset page should clear all data")
    public void testReset() {
        page.setPageId(123);
        page.markDirty();
        page.pin();

        page.reset();

        assertFalse(page.isDirty());
        assertFalse(page.isPinned());
        assertEquals(0, page.getPinCount());
    }

    @Test
    @DisplayName("Invalid page data size should throw exception")
    public void testInvalidPageSize() {
        byte[] invalidData = new byte[100];  // Wrong size

        assertThrows(IllegalArgumentException.class, () -> {
            new Page(invalidData);
        });
    }
}
