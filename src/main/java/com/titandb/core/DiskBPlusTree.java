package com.titandb.core;

import com.titandb.storage.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class DiskBPlusTree<K extends Comparable<K>, V> implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(DiskBPlusTree.class);

    private final int order;
    private final DiskManager diskManager;
    private int rootPageId;
    private boolean closed = false;

    private final Map<Integer, LeafNode<K, V>> nodeCache;
    private final Map<LeafNode<K, V>, Integer> nodeToPageId;

    public DiskBPlusTree(String fileName, int order) throws IOException {
        this.order = order;
        this.diskManager = new DiskManager(fileName);
        this.nodeCache = new HashMap<>();
        this.nodeToPageId = new HashMap<>();

        loadHeader();

        logger.info("Opened DiskBPlusTree: {} (order={}, root={})",
                fileName, order, rootPageId);
    }

    private void loadHeader() throws IOException {
        Page headerPage = diskManager.readPage(0);
        byte[] data = headerPage.getData();
        ByteBuffer buffer = ByteBuffer.wrap(data);

        buffer.position(16);  // Skip page header
        int version = buffer.getInt();
        this.rootPageId = buffer.getInt();

        logger.debug("Loaded header: version={}, rootPageId={}", version, rootPageId);
    }

    private void saveHeader() throws IOException {
        if (closed) return;

        Page headerPage = diskManager.readPage(0);
        byte[] data = headerPage.getData();
        ByteBuffer buffer = ByteBuffer.wrap(data);

        buffer.position(16);
        buffer.putInt(1);
        buffer.putInt(rootPageId);

        diskManager.writePage(0, headerPage);
        logger.debug("Saved header: rootPageId={}", rootPageId);
    }

    @SuppressWarnings("unchecked")
    private LeafNode<K, V> loadNode(int pageId) throws IOException {
        if (nodeCache.containsKey(pageId)) {
            return nodeCache.get(pageId);
        }

        Page page = diskManager.readPage(pageId);
        byte[] data = page.getData();

        // Read from data section
        ByteBuffer buffer = ByteBuffer.wrap(data);
        buffer.position(Page.HEADER_SIZE);  // Position 16

        int dataLength = buffer.remaining();
        byte[] nodeData = new byte[dataLength];
        buffer.get(nodeData);

        LeafNode<K, V> node = (LeafNode<K, V>) NodeSerializer.deserializeLeafNode(
                nodeData, order);

        nodeCache.put(pageId, node);
        nodeToPageId.put(node, pageId);

        logger.debug("Loaded node from page {}", pageId);
        return node;
    }

    @SuppressWarnings("unchecked")
    private void saveNode(LeafNode<K, V> node) throws IOException {
        if (closed) return;

        Integer pageId = nodeToPageId.get(node);
        if (pageId == null) {
            pageId = diskManager.allocatePage();
            nodeToPageId.put(node, pageId);
            nodeCache.put(pageId, node);
            logger.debug("Allocated new page {} for node", pageId);
        }

        byte[] nodeData = NodeSerializer.serializeLeafNode(
                (LeafNode<Integer, String>) node);

        Page page = new Page();
        page.setPageId(pageId);
        page.setPageType(Page.PageType.LEAF);

        byte[] pageBytes = page.getData();
        ByteBuffer buffer = ByteBuffer.wrap(pageBytes);
        buffer.position(Page.HEADER_SIZE);
        buffer.put(nodeData);

        diskManager.writePage(pageId, page);

        logger.debug("Saved node to page {} ({} bytes)", pageId, nodeData.length);
    }

    public void insert(K key, V value) throws IOException {
        if (closed) {
            throw new IllegalStateException("Tree is closed");
        }

        if (rootPageId == -1) {
            LeafNode<K, V> root = new LeafNode<>(order);
            root.insert(key, value);

            saveNode(root);
            rootPageId = nodeToPageId.get(root);
            saveHeader();

            logger.info("Created root at page {}", rootPageId);
            return;
        }

        LeafNode<K, V> root = loadNode(rootPageId);
        root.insert(key, value);
        saveNode(root);

        logger.debug("Inserted key={} into root", key);
    }

    public V search(K key) throws IOException {
        if (closed) {
            throw new IllegalStateException("Tree is closed");
        }

        if (rootPageId == -1) {
            return null;
        }

        LeafNode<K, V> root = loadNode(rootPageId);
        return root.search(key);
    }

    public int getRootPageId() {
        return rootPageId;
    }

    public String getStatistics() {
        return diskManager.toString();
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }

        closed = true;
        diskManager.flush();
        diskManager.close();

        logger.info("Closed DiskBPlusTree");
    }
}
