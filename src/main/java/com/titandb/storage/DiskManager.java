package com.titandb.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * DiskManager handles low-level disk I/O for the database.
 *
 * Responsibilities:
 * - Read/write pages to disk file
 * - Allocate new pages
 * - Track database file size
 * - Manage file creation and deletion
 *
 * File Format:
 * - Fixed-size pages (4096 bytes each)
 * - Sequential layout (Page N at offset N × 4096)
 * - Page 0 reserved for header/metadata
 *
 * Thread Safety:
 * - All methods are synchronized for thread safety
 * - Uses RandomAccessFile for concurrent reads
 *
 * @author Ujjwal
 */
public class DiskManager implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(DiskManager.class);

    /** Database file name */
    private final String fileName;

    /** Random access file for reading/writing */
    private final RandomAccessFile dbFile;

    /** Current number of pages in the database */
    private final AtomicInteger numPages;

    /** Total reads performed (for statistics) */
    private long readCount = 0;

    /** Total writes performed (for statistics) */
    private long writeCount = 0;

    /**
     * Create or open a database file.
     *
     * @param fileName Path to database file
     * @throws IOException If file cannot be created/opened
     */
    public DiskManager(String fileName) throws IOException {
        this.fileName = fileName;

        // Check if file exists
        Path filePath = Paths.get(fileName);
        boolean fileExists = Files.exists(filePath);

        // Open file in read-write mode
        this.dbFile = new RandomAccessFile(fileName, "rw");

        if (fileExists) {
            // Calculate number of pages from file size
            long fileSize = dbFile.length();
            int pages = (int) (fileSize / Page.PAGE_SIZE);
            this.numPages = new AtomicInteger(pages);

            logger.info("Opened existing database: {} ({} pages, {} bytes)",
                    fileName, pages, fileSize);
        } else {
            // New database file
            this.numPages = new AtomicInteger(0);

            // Create header page (Page 0)
            Page headerPage = createHeaderPage();
            writePage(0, headerPage);

            logger.info("Created new database: {}", fileName);
        }
    }

    /**
     * Create the header page (Page 0) for a new database.
     */
    private Page createHeaderPage() {
        Page page = new Page();
        page.setPageId(0);
        page.setPageType(Page.PageType.HEADER);

        // Get buffer and write metadata AT START of data section
        ByteBuffer buffer = ByteBuffer.wrap(page.getData());
        buffer.position(Page.HEADER_SIZE);  // Skip page header, start at data section

        buffer.putInt(1);   // Offset 16: Version
        buffer.putInt(-1);  // Offset 20: Root page ID (-1 = empty)
        buffer.putInt(1);   // Offset 24: Next page ID

        return page;
    }

    /**
     * Read a page from disk.
     *
     * @param pageId Page ID to read
     * @return Page object with data
     * @throws IOException If read fails
     * @throws IllegalArgumentException If pageId is invalid
     */
    public synchronized Page readPage(int pageId) throws IOException {
        if (pageId < 0) {
            throw new IllegalArgumentException("Invalid page ID: " + pageId);
        }

        if (pageId >= numPages.get()) {
            throw new IllegalArgumentException(
                    "Page ID " + pageId + " exceeds database size (" + numPages.get() + " pages)"
            );
        }

        // Calculate file offset
        long offset = (long) pageId * Page.PAGE_SIZE;

        // Read page data
        byte[] data = new byte[Page.PAGE_SIZE];
        dbFile.seek(offset);
        int bytesRead = dbFile.read(data);

        if (bytesRead != Page.PAGE_SIZE) {
            throw new IOException(
                    "Incomplete page read: expected " + Page.PAGE_SIZE + ", got " + bytesRead
            );
        }

        readCount++;
        logger.debug("Read page {} from offset {}", pageId, offset);

        return new Page(data);
    }

    /**
     * Write a page to disk.
     *
     * @param pageId Page ID to write
     * @param page Page object to write
     * @throws IOException If write fails
     */
    public synchronized void writePage(int pageId, Page page) throws IOException {
        if (pageId < 0) {
            throw new IllegalArgumentException("Invalid page ID: " + pageId);
        }

        // Update page ID in the page itself
        page.setPageId(pageId);

        // Calculate file offset
        long offset = (long) pageId * Page.PAGE_SIZE;

        // Write page data
        dbFile.seek(offset);
        dbFile.write(page.getData());

        // Update page count if necessary
        if (pageId >= numPages.get()) {
            numPages.set(pageId + 1);
        }

        writeCount++;
        logger.debug("Wrote page {} to offset {}", pageId, offset);

        // Clear dirty flag after successful write
        page.clearDirty();
    }

    /**
     * Allocate a new page in the database.
     *
     * @return New page ID
     */
    public synchronized int allocatePage() {
        int newPageId = numPages.getAndIncrement();
        logger.debug("Allocated new page: {}", newPageId);
        return newPageId;
    }

    /**
     * Get the total number of pages in the database.
     */
    public int getNumPages() {
        return numPages.get();
    }

    /**
     * Get the database file size in bytes.
     */
    public long getFileSize() throws IOException {
        return dbFile.length();
    }

    /**
     * Get total number of reads performed.
     */
    public long getReadCount() {
        return readCount;
    }

    /**
     * Get total number of writes performed.
     */
    public long getWriteCount() {
        return writeCount;
    }

    /**
     * Flush all buffered data to disk.
     * Forces all OS buffers to be written.
     */
    public synchronized void flush() throws IOException {
        dbFile.getFD().sync();
        logger.debug("Flushed database file to disk");
    }

    /**
     * Close the database file.
     * Flushes all pending writes before closing.
     */
    @Override
    public synchronized void close() throws IOException {
        flush();
        dbFile.close();
        logger.info("Closed database: {} ({} reads, {} writes)",
                fileName, readCount, writeCount);
    }

    /**
     * Delete the database file.
     * File must be closed before deletion.
     */
    public static void deleteDatabase(String fileName) throws IOException {
        Path path = Paths.get(fileName);
        if (Files.exists(path)) {
            Files.delete(path);
            logger.info("Deleted database: {}", fileName);
        }
    }

    /**
     * Check if a database file exists.
     */
    public static boolean databaseExists(String fileName) {
        return Files.exists(Paths.get(fileName));
    }

    @Override
    public String toString() {
        return String.format(
                "DiskManager[file=%s, pages=%d, reads=%d, writes=%d]",
                fileName, numPages.get(), readCount, writeCount
        );
    }
}
