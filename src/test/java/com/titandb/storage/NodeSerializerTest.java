package com.titandb.storage;

import com.titandb.core.LeafNode;
import com.titandb.core.InternalNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Node Serialization Tests")
public class NodeSerializerTest {

    @Test
    @DisplayName("Serialize and deserialize empty leaf node")
    public void testEmptyLeafNode() {
        LeafNode<Integer, String> original = new LeafNode<>(4);

        byte[] serialized = NodeSerializer.serializeLeafNode(original);
        LeafNode<Integer, String> deserialized =
                NodeSerializer.deserializeLeafNode(serialized, 4);

        assertEquals(0, deserialized.getKeyCount());
    }

    @Test
    @DisplayName("Serialize and deserialize leaf node with one entry")
    public void testSingleEntryLeafNode() {
        LeafNode<Integer, String> original = new LeafNode<>(4);
        original.insert(10, "Alice");

        byte[] serialized = NodeSerializer.serializeLeafNode(original);
        LeafNode<Integer, String> deserialized =
                NodeSerializer.deserializeLeafNode(serialized, 4);

        assertEquals(1, deserialized.getKeyCount());
        assertEquals("Alice", deserialized.search(10));
    }

    @Test
    @DisplayName("Serialize and deserialize leaf node with multiple entries")
    public void testMultipleEntriesLeafNode() {
        LeafNode<Integer, String> original = new LeafNode<>(4);
        original.insert(10, "Alice");
        original.insert(20, "Bob");
        original.insert(30, "Charlie");

        byte[] serialized = NodeSerializer.serializeLeafNode(original);
        LeafNode<Integer, String> deserialized =
                NodeSerializer.deserializeLeafNode(serialized, 4);

        assertEquals(3, deserialized.getKeyCount());
        assertEquals("Alice", deserialized.search(10));
        assertEquals("Bob", deserialized.search(20));
        assertEquals("Charlie", deserialized.search(30));
    }

    @Test
    @DisplayName("Serialize leaf node with null value")
    public void testNullValue() {
        LeafNode<Integer, String> original = new LeafNode<>(4);
        original.insert(10, null);
        original.insert(20, "Bob");

        byte[] serialized = NodeSerializer.serializeLeafNode(original);
        LeafNode<Integer, String> deserialized =
                NodeSerializer.deserializeLeafNode(serialized, 4);

        assertEquals(2, deserialized.getKeyCount());
        assertNull(deserialized.search(10));
        assertEquals("Bob", deserialized.search(20));
    }

    @Test
    @DisplayName("Serialize leaf node with long string values")
    public void testLongStringValues() {
        LeafNode<Integer, String> original = new LeafNode<>(4);
        String longValue = "This is a very long string value that contains many characters!";
        original.insert(100, longValue);

        byte[] serialized = NodeSerializer.serializeLeafNode(original);
        LeafNode<Integer, String> deserialized =
                NodeSerializer.deserializeLeafNode(serialized, 4);

        assertEquals(longValue, deserialized.search(100));
    }

    @Test
    @DisplayName("Serialize and deserialize internal node")
    public void testInternalNode() {
        // Create internal node and add keys using public method
        InternalNode<Integer, String> original = new InternalNode<>(4);
        original.addKey(50);   // ✅ Use addKey() method
        original.addKey(100);
        original.addKey(150);

        byte[] serialized = NodeSerializer.serializeInternalNode(original);
        InternalNode<Integer, String> deserialized =
                NodeSerializer.deserializeInternalNode(serialized, 4);

        assertEquals(3, deserialized.getKeyCount());
        assertEquals(Integer.valueOf(50), deserialized.getKey(0));
        assertEquals(Integer.valueOf(100), deserialized.getKey(1));
        assertEquals(Integer.valueOf(150), deserialized.getKey(2));
    }

    @Test
    @DisplayName("Serialized data should fit in a page")
    public void testSerializedSizeFitsPage() {
        LeafNode<Integer, String> node = new LeafNode<>(4);

        // Add maximum entries for order-4 tree
        node.insert(10, "value1");
        node.insert(20, "value2");
        node.insert(30, "value3");

        byte[] serialized = NodeSerializer.serializeLeafNode(node);

        // Should fit in page data section
        assertTrue(serialized.length <= Page.DATA_SIZE,
                "Serialized node (" + serialized.length + " bytes) exceeds page size");
    }

    @Test
    @DisplayName("Round-trip serialization preserves key order")
    public void testKeyOrderPreserved() {
        LeafNode<Integer, String> original = new LeafNode<>(4);
        original.insert(30, "C");
        original.insert(10, "A");
        original.insert(20, "B");

        byte[] serialized = NodeSerializer.serializeLeafNode(original);
        LeafNode<Integer, String> deserialized =
                NodeSerializer.deserializeLeafNode(serialized, 4);

        // Keys should be in sorted order
        assertEquals(Integer.valueOf(10), deserialized.getKey(0));
        assertEquals(Integer.valueOf(20), deserialized.getKey(1));
        assertEquals(Integer.valueOf(30), deserialized.getKey(2));
    }

    @Test
    @DisplayName("isLeafNode correctly identifies node type")
    public void testNodeTypeDetection() {
        LeafNode<Integer, String> leaf = new LeafNode<>(4);
        leaf.insert(10, "test");

        InternalNode<Integer, String> internal = new InternalNode<>(4);
        internal.addKey(50);  // ✅ Use addKey() method

        byte[] leafBytes = NodeSerializer.serializeLeafNode(leaf);
        byte[] internalBytes = NodeSerializer.serializeInternalNode(internal);

        assertTrue(NodeSerializer.isLeafNode(leafBytes));
        assertFalse(NodeSerializer.isLeafNode(internalBytes));
    }

    @Test
    @DisplayName("Empty internal node serialization")
    public void testEmptyInternalNode() {
        InternalNode<Integer, String> original = new InternalNode<>(4);

        byte[] serialized = NodeSerializer.serializeInternalNode(original);
        InternalNode<Integer, String> deserialized =
                NodeSerializer.deserializeInternalNode(serialized, 4);

        assertEquals(0, deserialized.getKeyCount());
    }
}
