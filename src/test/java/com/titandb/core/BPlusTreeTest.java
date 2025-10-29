package com.titandb.core;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive test suite for BPlusTree.
 *
 * Test Categories:
 * 1. Basic CRUD operations
 * 2. Tree structure validation
 * 3. Edge cases (empty, single element, duplicates)
 * 4. Node splitting behavior
 * 5. Range scans
 * 6. Large-scale stress tests
 *
 * Test Strategy: Test-Driven Development
 * - Each test is independent (uses fresh tree)
 * - Tests are organized by functionality
 * - Edge cases are explicitly covered
 */
@DisplayName("B+ Tree Core Functionality Tests")
public class BPlusTreeTest {

    private BPlusTree<Integer, String> tree;

    /**
     * Set up a fresh tree before each test.
     * Using order=4 for easy visualization and debugging.
     */
    @BeforeEach
    public void setUp() {
        tree = new BPlusTree<>(4);
    }

    // ==================== BASIC OPERATIONS ====================

    @Test
    @DisplayName("Empty tree should have size 0")
    public void testEmptyTree() {
        assertEquals(0, tree.size());
        assertTrue(tree.isEmpty());
        assertNull(tree.search(1), "Search in empty tree should return null");
    }

    @Test
    @DisplayName("Single insert and search")
    public void testSingleInsertAndSearch() {
        // Insert one key-value pair
        assertTrue(tree.insert(10, "ten"), "First insert should return true");

        // Verify state
        assertEquals(1, tree.size());
        assertFalse(tree.isEmpty());

        // Search for existing key
        assertEquals("ten", tree.search(10));

        // Search for non-existing key
        assertNull(tree.search(999));
    }

    @Test
    @DisplayName("Multiple inserts without splitting")
    public void testMultipleInsertsNoSplit() {
        // Insert 3 keys (order=4, so no split yet)
        tree.insert(10, "ten");
        tree.insert(20, "twenty");
        tree.insert(30, "thirty");

        assertEquals(3, tree.size());
        assertEquals("ten", tree.search(10));
        assertEquals("twenty", tree.search(20));
        assertEquals("thirty", tree.search(30));

        // Root should still be a leaf
        assertTrue(tree.getRoot().isLeaf());
    }

    @Test
    @DisplayName("Insert duplicate key should update value")
    public void testDuplicateInsertUpdatesValue() {
        // First insert
        assertTrue(tree.insert(10, "original"), "First insert should return true");
        assertEquals("original", tree.search(10));
        assertEquals(1, tree.size());

        // Duplicate insert (should update, not insert)
        assertFalse(tree.insert(10, "updated"), "Duplicate insert should return false");
        assertEquals("updated", tree.search(10), "Value should be updated");
        assertEquals(1, tree.size(), "Size should not increase");
    }

    @Test
    @DisplayName("Insert in reverse order")
    public void testInsertReverseOrder() {
        // Insert keys in descending order
        tree.insert(40, "forty");
        tree.insert(30, "thirty");
        tree.insert(20, "twenty");
        tree.insert(10, "ten");

        assertEquals(4, tree.size());

        // Keys should still be searchable
        assertEquals("ten", tree.search(10));
        assertEquals("twenty", tree.search(20));
        assertEquals("thirty", tree.search(30));
        assertEquals("forty", tree.search(40));
    }

    // ==================== NODE SPLITTING ====================

    @Test
    @DisplayName("Leaf node split when full")
    public void testLeafNodeSplit() {
        // Order=4, so inserting 4th element triggers split
        tree.insert(10, "a");
        tree.insert(20, "b");
        tree.insert(30, "c");

        // Root is still a leaf
        assertTrue(tree.getRoot().isLeaf());

        // 4th insert triggers split
        tree.insert(40, "d");

        // Root should now be internal node
        assertFalse(tree.getRoot().isLeaf(), "Root should become internal after split");

        // All keys should still be searchable
        assertEquals("a", tree.search(10));
        assertEquals("b", tree.search(20));
        assertEquals("c", tree.search(30));
        assertEquals("d", tree.search(40));

        assertEquals(4, tree.size());
    }

    @Test
    @DisplayName("Multiple splits create proper tree structure")
    public void testMultipleSplits() {
        // Insert enough keys to trigger multiple splits
        for (int i = 10; i <= 100; i += 10) {
            tree.insert(i, "value_" + i);
        }

        assertEquals(10, tree.size());

        // Verify all keys are searchable
        for (int i = 10; i <= 100; i += 10) {
            assertEquals("value_" + i, tree.search(i), "Key " + i + " should be found");
        }

        // Root should be internal
        assertFalse(tree.getRoot().isLeaf());
    }

    @Test
    @DisplayName("Insert random order triggers proper rebalancing")
    public void testRandomInsertOrder() {
        List<Integer> keys = new ArrayList<>();
        for (int i = 1; i <= 20; i++) {
            keys.add(i * 5);
        }

        // Shuffle keys
        Collections.shuffle(keys, new Random(42)); // Fixed seed for reproducibility

        // Insert in random order
        for (Integer key : keys) {
            tree.insert(key, "value_" + key);
        }

        assertEquals(20, tree.size());

        // Verify all keys are searchable
        for (Integer key : keys) {
            assertNotNull(tree.search(key), "Key " + key + " should be found");
        }
    }

    // ==================== DELETION ====================

    @Test
    @DisplayName("Delete from single-element tree")
    public void testDeleteSingleElement() {
        tree.insert(10, "ten");

        assertEquals("ten", tree.delete(10), "Delete should return deleted value");
        assertEquals(0, tree.size());
        assertNull(tree.search(10), "Deleted key should not be found");
    }

    @Test
    @DisplayName("Delete non-existing key returns null")
    public void testDeleteNonExisting() {
        tree.insert(10, "ten");
        tree.insert(20, "twenty");

        assertNull(tree.delete(999), "Deleting non-existing key should return null");
        assertEquals(2, tree.size(), "Size should not change");
    }

    @Test
    @DisplayName("Delete and re-insert same key")
    public void testDeleteAndReinsert() {
        tree.insert(10, "original");
        assertEquals("original", tree.search(10));

        tree.delete(10);
        assertNull(tree.search(10));

        tree.insert(10, "new");
        assertEquals("new", tree.search(10));
        assertEquals(1, tree.size());
    }

    // ==================== RANGE SCANS ====================

    @Test
    @DisplayName("Range scan on empty tree")
    public void testRangeScanEmpty() {
        List<Entry<Integer, String>> results = tree.rangeScan(0, 100);
        assertTrue(results.isEmpty());
    }

    @Test
    @DisplayName("Range scan returns entries in sorted order")
    public void testRangeScanSorted() {
        // Insert in random order
        tree.insert(30, "thirty");
        tree.insert(10, "ten");
        tree.insert(50, "fifty");
        tree.insert(20, "twenty");
        tree.insert(40, "forty");

        // Scan range [15, 45)
        List<Entry<Integer, String>> results = tree.rangeScan(15, 45);

        assertEquals(3, results.size());
        assertEquals(Integer.valueOf(20), results.get(0).getKey());
        assertEquals(Integer.valueOf(30), results.get(1).getKey());
        assertEquals(Integer.valueOf(40), results.get(2).getKey());
    }

    @Test
    @DisplayName("Range scan with no matching keys")
    public void testRangeScanNoMatches() {
        tree.insert(10, "ten");
        tree.insert(50, "fifty");

        // Range with no keys
        List<Entry<Integer, String>> results = tree.rangeScan(20, 40);
        assertTrue(results.isEmpty());
    }

    @Test
    @DisplayName("Range scan full tree")
    public void testRangeScanFullTree() {
        for (int i = 10; i <= 50; i += 10) {
            tree.insert(i, "value_" + i);
        }

        List<Entry<Integer, String>> results = tree.rangeScan(0, 100);
        assertEquals(5, results.size());

        // Verify sorted order
        for (int i = 0; i < results.size() - 1; i++) {
            assertTrue(results.get(i).getKey() < results.get(i + 1).getKey());
        }
    }

    @Test
    @DisplayName("Get all entries returns full sorted list")
    public void testGetAllEntries() {
        tree.insert(30, "thirty");
        tree.insert(10, "ten");
        tree.insert(20, "twenty");

        List<Entry<Integer, String>> all = tree.getAllEntries();

        assertEquals(3, all.size());
        assertEquals(Integer.valueOf(10), all.get(0).getKey());
        assertEquals(Integer.valueOf(20), all.get(1).getKey());
        assertEquals(Integer.valueOf(30), all.get(2).getKey());
    }

    // ==================== PARAMETERIZED TESTS ====================

    @ParameterizedTest(name = "Insert and search key {0}")
    @ValueSource(ints = {1, 100, 999, -50, 0})
    @DisplayName("Parameterized insert and search")
    public void testInsertSearchParameterized(int key) {
        String value = "value_" + key;
        tree.insert(key, value);
        assertEquals(value, tree.search(key));
    }

    @ParameterizedTest(name = "Order {0} tree handles inserts")
    @ValueSource(ints = {3, 4, 5, 10, 100})
    @DisplayName("Different tree orders work correctly")
    public void testDifferentOrders(int order) {
        BPlusTree<Integer, String> customTree = new BPlusTree<>(order);

        // Insert 20 keys
        for (int i = 1; i <= 20; i++) {
            customTree.insert(i, "value_" + i);
        }

        assertEquals(20, customTree.size());

        // Verify all searchable
        for (int i = 1; i <= 20; i++) {
            assertNotNull(customTree.search(i));
        }
    }

    // ==================== EDGE CASES ====================

    @Test
    @DisplayName("Insert null key throws exception")
    public void testInsertNullKey() {
        assertThrows(IllegalArgumentException.class, () -> {
            tree.insert(null, "value");
        });
    }

    @Test
    @DisplayName("Search null key throws exception")
    public void testSearchNullKey() {
        assertThrows(IllegalArgumentException.class, () -> {
            tree.search(null);
        });
    }

    @Test
    @DisplayName("Range scan with invalid range throws exception")
    public void testRangeScanInvalidRange() {
        assertThrows(IllegalArgumentException.class, () -> {
            tree.rangeScan(100, 10); // startKey > endKey
        });
    }

    @Test
    @DisplayName("Insert null value is allowed")
    public void testInsertNullValue() {
        // Null values should be allowed (like SQL NULL)
        assertDoesNotThrow(() -> tree.insert(10, null));
        assertNull(tree.search(10));
        assertEquals(1, tree.size());
    }

    // ==================== STRESS TESTS ====================

    @Test
    @DisplayName("Stress test: 1000 sequential inserts")
    public void testSequentialInserts1000() {
        for (int i = 0; i < 1000; i++) {
            tree.insert(i, "value_" + i);
        }

        assertEquals(1000, tree.size());

        // Spot check some keys
        assertEquals("value_0", tree.search(0));
        assertEquals("value_500", tree.search(500));
        assertEquals("value_999", tree.search(999));

        // Range scan
        List<Entry<Integer, String>> range = tree.rangeScan(100, 200);
        assertEquals(100, range.size());
    }

    @Test
    @DisplayName("Stress test: 10000 random inserts")
    public void testRandomInserts10000() {
        Random rand = new Random(123); // Fixed seed

        for (int i = 0; i < 10000; i++) {
            int key = rand.nextInt(100000);
            tree.insert(key, "value_" + key);
        }

        // Size may be less than 10000 due to duplicates
        assertTrue(tree.size() <= 10000);
        assertTrue(tree.size() > 0);

        // Verify some random searches don't crash
        for (int i = 0; i < 100; i++) {
            int key = rand.nextInt(100000);
            tree.search(key); // Just verify no exception
        }
    }

    @Test
    @DisplayName("Stress test: Alternating insert and delete")
    public void testAlternatingInsertDelete() {
        // Insert 100 keys
        for (int i = 0; i < 100; i++) {
            tree.insert(i, "value_" + i);
        }

        // Delete every other key
        for (int i = 0; i < 100; i += 2) {
            tree.delete(i);
        }

        assertEquals(50, tree.size());

        // Verify deleted keys are gone
        assertNull(tree.search(0));
        assertNull(tree.search(50));

        // Verify remaining keys exist
        assertEquals("value_1", tree.search(1));
        assertEquals("value_51", tree.search(51));
    }

    // ==================== TREE STRUCTURE VALIDATION ====================

    @Test
    @DisplayName("Root starts as leaf node")
    public void testRootStartsAsLeaf() {
        assertTrue(tree.getRoot().isLeaf());
        assertTrue(tree.getRoot() instanceof LeafNode);
    }

    @Test
    @DisplayName("Root becomes internal after first split")
    public void testRootBecomesInternal() {
        // Fill root until it splits
        tree.insert(10, "a");
        tree.insert(20, "b");
        tree.insert(30, "c");
        tree.insert(40, "d"); // Triggers split

        assertFalse(tree.getRoot().isLeaf());
        assertTrue(tree.getRoot() instanceof InternalNode);
    }

    @Test
    @DisplayName("Print tree doesn't crash")
    public void testPrintTree() {
        tree.insert(10, "ten");
        tree.insert(20, "twenty");
        tree.insert(30, "thirty");
        tree.insert(40, "forty");

        // Just verify printTree doesn't throw exception
        assertDoesNotThrow(() -> tree.printTree());
    }

    // ==================== INTEGRATION TESTS ====================

    @Test
    @DisplayName("Complex scenario: Insert, search, delete, range scan")
    public void testComplexScenario() {
        // 1. Insert 20 keys
        for (int i = 1; i <= 20; i++) {
            tree.insert(i * 10, "value_" + (i * 10));
        }
        assertEquals(20, tree.size());

        // 2. Search some keys
        assertEquals("value_50", tree.search(50));
        assertEquals("value_150", tree.search(150));
        assertNull(tree.search(999));

        // 3. Delete some keys
        tree.delete(50);
        tree.delete(100);
        tree.delete(150);
        assertEquals(17, tree.size());

        // 4. Range scan
        List<Entry<Integer, String>> range = tree.rangeScan(60, 140);
        assertEquals(7, range.size()); // 70, 80, 90, 110, 120, 130

        // 5. Insert new keys in deleted range
        tree.insert(50, "new_50");
        tree.insert(100, "new_100");

        assertEquals(19, tree.size());
        assertEquals("new_50", tree.search(50));
        assertEquals("new_100", tree.search(100));
    }
}
