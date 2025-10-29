package com.titandb.storage;

import com.titandb.core.LeafNode;
import com.titandb.core.InternalNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Page + Node Integration Tests")
public class PageNodeIntegrationTest {

    @Test
    @DisplayName("Store and load leaf node in a page")
    public void testStoreLeafNodeInPage() {
        // Create a leaf node
        LeafNode<Integer, String> original = new LeafNode<>(4);
        original.insert(10, "Alice");
        original.insert(20, "Bob");
        original.insert(30, "Charlie");

        // Serialize to bytes
        byte[] nodeBytes = NodeSerializer.serializeLeafNode(original);

        // Create a page and store the node
        Page page = new Page();
        page.setPageId(1);
        page.setPageType(Page.PageType.LEAF);

        // Write node data to page
        ByteBuffer buffer = page.getDataBuffer();
        buffer.put(nodeBytes);

        // Simulate writing to disk and reading back
        byte[] diskData = page.getData();

        // Load page from "disk"
        Page loadedPage = new Page(diskData);

        assertEquals(1, loadedPage.getPageId());
        assertEquals(Page.PageType.LEAF, loadedPage.getPageType());

        // Extract node data
        ByteBuffer loadedBuffer = loadedPage.getDataBuffer();
        byte[] loadedNodeBytes = new byte[nodeBytes.length];
        loadedBuffer.get(loadedNodeBytes);

        // Deserialize node
        LeafNode<Integer, String> loadedNode =
                NodeSerializer.deserializeLeafNode(loadedNodeBytes, 4);

        // Verify data integrity
        assertEquals(3, loadedNode.getKeyCount());
        assertEquals("Alice", loadedNode.search(10));
        assertEquals("Bob", loadedNode.search(20));
        assertEquals("Charlie", loadedNode.search(30));
    }

    @Test
    @DisplayName("Store and load internal node in a page")
    public void testStoreInternalNodeInPage() {
        // Create internal node
        InternalNode<Integer, String> original = new InternalNode<>(4);
        original.addKey(50);
        original.addKey(100);

        // Serialize to bytes
        byte[] nodeBytes = NodeSerializer.serializeInternalNode(original);

        // Create a page
        Page page = new Page();
        page.setPageId(2);
        page.setPageType(Page.PageType.INTERNAL);

        // Write to page
        ByteBuffer buffer = page.getDataBuffer();
        buffer.put(nodeBytes);

        // Simulate disk round-trip
        byte[] diskData = page.getData();
        Page loadedPage = new Page(diskData);

        assertEquals(2, loadedPage.getPageId());
        assertEquals(Page.PageType.INTERNAL, loadedPage.getPageType());

        // Deserialize
        ByteBuffer loadedBuffer = loadedPage.getDataBuffer();
        byte[] loadedNodeBytes = new byte[nodeBytes.length];
        loadedBuffer.get(loadedNodeBytes);

        InternalNode<Integer, String> loadedNode =
                NodeSerializer.deserializeInternalNode(loadedNodeBytes, 4);

        assertEquals(2, loadedNode.getKeyCount());
        assertEquals(Integer.valueOf(50), loadedNode.getKey(0));
        assertEquals(Integer.valueOf(100), loadedNode.getKey(1));
    }

    @Test
    @DisplayName("Multiple nodes fit in separate pages")
    public void testMultipleNodesMultiplePages() {
        // Create two leaf nodes
        LeafNode<Integer, String> node1 = new LeafNode<>(4);
        node1.insert(10, "Node1-Value1");
        node1.insert(20, "Node1-Value2");

        LeafNode<Integer, String> node2 = new LeafNode<>(4);
        node2.insert(100, "Node2-Value1");
        node2.insert(200, "Node2-Value2");

        // Store in separate pages
        Page page1 = new Page();
        page1.setPageId(1);
        page1.setPageType(Page.PageType.LEAF);
        ByteBuffer buffer1 = page1.getDataBuffer();
        buffer1.put(NodeSerializer.serializeLeafNode(node1));

        Page page2 = new Page();
        page2.setPageId(2);
        page2.setPageType(Page.PageType.LEAF);
        ByteBuffer buffer2 = page2.getDataBuffer();
        buffer2.put(NodeSerializer.serializeLeafNode(node2));

        // Verify both pages are independent
        assertEquals(1, page1.getPageId());
        assertEquals(2, page2.getPageId());

        // Simulate disk operations
        byte[] disk1 = page1.getData();
        byte[] disk2 = page2.getData();

        // Load and verify
        Page loaded1 = new Page(disk1);
        Page loaded2 = new Page(disk2);

        assertEquals(1, loaded1.getPageId());
        assertEquals(2, loaded2.getPageId());
    }

    @Test
    @DisplayName("Page dirty flag works with node modifications")
    public void testPageDirtyFlag() {
        LeafNode<Integer, String> node = new LeafNode<>(4);
        node.insert(10, "test");

        Page page = new Page();

        // Initially not dirty
        assertFalse(page.isDirty());

        // Setting page ID marks dirty
        page.setPageId(1);
        assertTrue(page.isDirty()); // ✅ FIXED: setPageId() marks dirty

        // Clear dirty flag
        page.clearDirty();
        assertFalse(page.isDirty());

        // Setting type also marks dirty
        page.setPageType(Page.PageType.LEAF);
        assertTrue(page.isDirty());

        page.clearDirty();
        assertFalse(page.isDirty());

        // Write node data and mark dirty
        ByteBuffer buffer = page.getDataBuffer();
        buffer.put(NodeSerializer.serializeLeafNode(node));
        page.markDirty();

        assertTrue(page.isDirty());
    }


    @Test
    @DisplayName("Large leaf node with many entries fits in page")
    public void testLargeLeafNode() {
        LeafNode<Integer, String> node = new LeafNode<>(100); // Larger order

        // Insert many entries
        for (int i = 0; i < 50; i++) {
            node.insert(i * 10, "Value_" + i);
        }

        byte[] serialized = NodeSerializer.serializeLeafNode(node);

        // Should fit in page
        assertTrue(serialized.length <= Page.DATA_SIZE,
                "Large node doesn't fit in page: " + serialized.length + " bytes");

        // Verify can be stored and loaded
        Page page = new Page();
        page.setPageId(99);
        page.setPageType(Page.PageType.LEAF);

        ByteBuffer buffer = page.getDataBuffer();
        buffer.put(serialized);

        // Round-trip
        byte[] diskData = page.getData();
        Page loadedPage = new Page(diskData);

        ByteBuffer loadedBuffer = loadedPage.getDataBuffer();
        byte[] loadedBytes = new byte[serialized.length];
        loadedBuffer.get(loadedBytes);

        LeafNode<Integer, String> loadedNode =
                NodeSerializer.deserializeLeafNode(loadedBytes, 100);

        assertEquals(50, loadedNode.getKeyCount());
        assertEquals("Value_0", loadedNode.search(0));
        assertEquals("Value_49", loadedNode.search(490));
    }
}
