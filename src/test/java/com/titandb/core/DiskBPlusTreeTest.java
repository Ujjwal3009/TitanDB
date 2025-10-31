package com.titandb.core;

import com.titandb.storage.DiskManager;
import org.junit.jupiter.api.*;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("DiskBPlusTree Tests")
class DiskBPlusTreeTest {

    private static final String TEST_DB = "test-tree.titandb";
    private DiskBPlusTree<Integer, String> tree;

    @BeforeEach
    void setUp() throws IOException {
        cleanup();
        tree = new DiskBPlusTree<>(TEST_DB, 4);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (tree != null) {
            tree.close();
        }
        cleanup();
    }

    void cleanup() throws IOException {
        if (DiskManager.databaseExists(TEST_DB)) {
            DiskManager.deleteDatabase(TEST_DB);
        }
    }

    @Test
    @DisplayName("Insert and search single key")
    void testInsertSearchSingle() throws IOException {
        tree.insert(10, "Alice");
        assertEquals("Alice", tree.search(10));
    }

    @Test
    @DisplayName("Insert and search multiple keys")
    void testInsertSearchMultiple() throws IOException {
        tree.insert(10, "Alice");
        tree.insert(20, "Bob");
        tree.insert(30, "Charlie");

        assertEquals("Alice", tree.search(10));
        assertEquals("Bob", tree.search(20));
        assertEquals("Charlie", tree.search(30));
    }

    @Test
    @DisplayName("Search non-existent key")
    void testSearchNonExistent() throws IOException {
        tree.insert(10, "Alice");
        assertNull(tree.search(99));
    }

    @Test
    @DisplayName("Persistence across restarts")
    void testPersistence() throws IOException {
        tree.insert(42, "Answer");
        tree.close();

        tree = new DiskBPlusTree<>(TEST_DB, 4);
        assertEquals("Answer", tree.search(42));
    }

    @Test
    @DisplayName("Statistics available")
    void testStatistics() throws IOException {
        tree.insert(1, "One");
        String stats = tree.getStatistics();
        assertNotNull(stats);
        assertTrue(stats.length() > 0);
    }

    // ========== WEEK 4 LSN TESTS ==========

    @Test
    @DisplayName("Week 4: Insert with LSN tracking")
    void testInsertWithLSN() throws IOException {
        tree.insertWithLSN(10, "Alice", 1001L);
        tree.insertWithLSN(20, "Bob", 1002L);

        assertEquals("Alice", tree.search(10));
        assertEquals("Bob", tree.search(20));
        assertEquals(1002L, tree.getRootPageLSN());
    }

    @Test
    @DisplayName("Week 4: LSN increases with inserts")
    void testLSNIncreases() throws IOException {
        tree.insertWithLSN(1, "One", 100L);
        assertEquals(100L, tree.getRootPageLSN());

        tree.insertWithLSN(2, "Two", 200L);
        assertEquals(200L, tree.getRootPageLSN());

        tree.insertWithLSN(3, "Three", 300L);
        assertEquals(300L, tree.getRootPageLSN());
    }

    @Test
    @DisplayName("Week 4: LSN persists across restarts")
    void testLSNPersistence() throws IOException {
        tree.insertWithLSN(42, "Answer", 9999L);
        assertEquals(9999L, tree.getRootPageLSN());
        tree.close();

        tree = new DiskBPlusTree<>(TEST_DB, 4);

        assertEquals(9999L, tree.getRootPageLSN());
        assertEquals("Answer", tree.search(42));
    }

    @Test
    @DisplayName("Week 4: Statistics include LSN")
    void testStatisticsWithLSN() throws IOException {
        tree.insertWithLSN(10, "Test", 5000L);

        String stats = tree.getStatistics();
        assertTrue(stats.contains("Root Page LSN: 5000"));
    }

    @Test
    @DisplayName("Week 4: Regular insert doesn't set LSN")
    void testRegularInsertNoLSN() throws IOException {
        tree.insert(10, "Alice");
        assertEquals(-1L, tree.getRootPageLSN());
    }

    @Test
    @DisplayName("Week 4: Regular inserts preserve last LSN")
    void testRegularInsertsPreserveLSN() throws IOException {
        // First, set LSN
        tree.insertWithLSN(10, "Alice", 1000L);
        assertEquals(1000L, tree.getRootPageLSN());

        // Regular inserts should NOT change LSN
        tree.insert(20, "Bob");
        assertEquals(1000L, tree.getRootPageLSN());  // Still 1000, not -1!

        tree.insert(30, "Charlie");
        assertEquals(1000L, tree.getRootPageLSN());  // Still 1000!

        // Only LSN insert changes it
        tree.insertWithLSN(40, "David", 2000L);
        assertEquals(2000L, tree.getRootPageLSN());

        // All data should be there
        assertEquals("Alice", tree.search(10));
        assertEquals("Bob", tree.search(20));
        assertEquals("Charlie", tree.search(30));
        assertEquals("David", tree.search(40));
    }
}
