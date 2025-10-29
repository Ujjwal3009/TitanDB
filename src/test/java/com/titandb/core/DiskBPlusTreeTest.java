package com.titandb.core;

import com.titandb.storage.DiskManager;
import org.junit.jupiter.api.*;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Disk-Based B+ Tree Integration Tests")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class DiskBPlusTreeTest {

    private static final String TEST_DB = "test_diskbptree.titandb";
    private DiskBPlusTree<Integer, String> tree;

    @BeforeEach
    public void setUp() throws IOException {
        // Delete test database if exists
        if (DiskManager.databaseExists(TEST_DB)) {
            DiskManager.deleteDatabase(TEST_DB);
        }
    }

    @AfterEach
    public void tearDown() throws IOException {
        if (tree != null) {
            try {
                tree.close();
            } catch (Exception e) {
                // Ignore if already closed
            }
            tree = null;
        }

        // Clean up test file
        if (DiskManager.databaseExists(TEST_DB)) {
            DiskManager.deleteDatabase(TEST_DB);
        }
    }

    @Test
    @Order(1)
    @DisplayName("Create new disk-based tree")
    public void testCreateTree() throws IOException {
        tree = new DiskBPlusTree<>(TEST_DB, 4);

        assertNotNull(tree);
        assertEquals(-1, tree.getRootPageId()); // Empty tree
    }

    @Test
    @Order(2)
    @DisplayName("Insert single entry and save to disk")
    public void testInsertSingleEntry() throws IOException {
        tree = new DiskBPlusTree<>(TEST_DB, 4);
        tree.insert(10, "Alice");
        tree.close();

        // Verify file was created
        assertTrue(DiskManager.databaseExists(TEST_DB));
    }

    @Test
    @Order(3)
    @DisplayName("Insert and search in same session")
    public void testInsertAndSearch() throws IOException {
        tree = new DiskBPlusTree<>(TEST_DB, 4);

        tree.insert(10, "Alice");
        tree.insert(20, "Bob");
        tree.insert(30, "Charlie");

        assertEquals("Alice", tree.search(10));
        assertEquals("Bob", tree.search(20));
        assertEquals("Charlie", tree.search(30));
        assertNull(tree.search(999));
    }

    @Test
    @Order(4)
    @DisplayName("Data persists across restarts")
    public void testPersistenceAcrossRestarts() throws IOException {
        // Session 1: Insert data
        tree = new DiskBPlusTree<>(TEST_DB, 4);
        tree.insert(10, "Alice");
        tree.insert(20, "Bob");
        tree.insert(30, "Charlie");
        int rootId = tree.getRootPageId();
        tree.close();
        tree = null; // Explicitly null

        // Session 2: Reopen and verify
        tree = new DiskBPlusTree<>(TEST_DB, 4);
        assertEquals(rootId, tree.getRootPageId(), "Root page ID should persist");
        assertEquals("Alice", tree.search(10), "Should find Alice");
        assertEquals("Bob", tree.search(20), "Should find Bob");
        assertEquals("Charlie", tree.search(30), "Should find Charlie");
    }

    @Test
    @Order(5)
    @DisplayName("Multiple inserts persist")
    public void testMultipleInsertsPersist() throws IOException {
        // Insert 10 entries
        tree = new DiskBPlusTree<>(TEST_DB, 4);
        for (int i = 0; i < 10; i++) {
            tree.insert(i * 10, "Value_" + i);
        }
        tree.close();
        tree = null;

        // Reopen and verify all
        tree = new DiskBPlusTree<>(TEST_DB, 4);
        for (int i = 0; i < 10; i++) {
            assertEquals("Value_" + i, tree.search(i * 10),
                    "Should find Value_" + i + " for key " + (i * 10));
        }
    }

    @Test
    @Order(6)
    @DisplayName("Root page ID is saved and loaded")
    public void testRootPageIdPersistence() throws IOException {
        tree = new DiskBPlusTree<>(TEST_DB, 4);
        assertEquals(-1, tree.getRootPageId()); // Empty

        tree.insert(10, "test");
        int rootId = tree.getRootPageId();
        assertTrue(rootId > 0, "Root page should be allocated"); // Should have allocated a page
        tree.close();
        tree = null;

        // Reopen
        tree = new DiskBPlusTree<>(TEST_DB, 4);
        assertEquals(rootId, tree.getRootPageId(), "Root page ID should be same after reopen"); // Same root ID
    }

    @Test
    @Order(7)
    @DisplayName("Statistics tracking")
    public void testStatistics() throws IOException {
        tree = new DiskBPlusTree<>(TEST_DB, 4);
        tree.insert(10, "test");

        String stats = tree.getStatistics();
        assertNotNull(stats);
        assertTrue(stats.contains("reads"));
        assertTrue(stats.contains("writes"));
    }

    @Test
    @Order(8)
    @DisplayName("Empty tree returns null for search")
    public void testSearchEmptyTree() throws IOException {
        tree = new DiskBPlusTree<>(TEST_DB, 4);
        assertNull(tree.search(10));
    }
}
