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
    private final BufferPoolManager bufferPool;
    private final boolean useCache;

    private int rootPageId;
    private boolean closed = false;

    private final Map<Integer, LeafNode<K, V>> nodeCache;
    private final Map<LeafNode<K, V>, Integer> nodeToPageId;

    public DiskBPlusTree(String fileName, int order) throws IOException {
        this(fileName, order, true);
    }

    public DiskBPlusTree(String fileName, int order, boolean useCache) throws IOException {
        this.order = order;
        this.useCache = useCache;
        this.diskManager = new DiskManager(fileName);
        this.bufferPool = useCache ? new BufferPoolManager(1000, diskManager) : null;
        this.nodeCache = new HashMap<>();
        this.nodeToPageId = new HashMap<>();

        loadHeader();

        logger.info("Opened DiskBPlusTree: {} (order={}, root={}, cache={})",
                fileName, order, rootPageId, useCache ? "ON" : "OFF");
    }

    private void loadHeader() throws IOException {
        Page headerPage;
        if (useCache) {
            headerPage = bufferPool.fetchPage(0);
        } else {
            headerPage = diskManager.readPage(0);
        }

        byte[] data = headerPage.getData();
        ByteBuffer buffer = ByteBuffer.wrap(data);
        buffer.position(16);

        int version = buffer.getInt();
        this.rootPageId = buffer.getInt();

        if (useCache) {
            bufferPool.unpinPage(0, false);
        }

        logger.debug("Loaded header: version={}, rootPageId={}", version, rootPageId);
    }

    private void saveHeader() throws IOException {
        if (closed) return;

        Page headerPage;
        if (useCache) {
            headerPage = bufferPool.fetchPage(0);
        } else {
            headerPage = diskManager.readPage(0);
        }

        byte[] data = headerPage.getData();
        ByteBuffer buffer = ByteBuffer.wrap(data);
        buffer.position(16);

        buffer.putInt(1);
        buffer.putInt(rootPageId);

        if (useCache) {
            bufferPool.unpinPage(0, true);
        } else {
            diskManager.writePage(0, headerPage);
        }

        logger.debug("Saved header: rootPageId={}", rootPageId);
    }

    @SuppressWarnings("unchecked")
    private LeafNode<K, V> loadNode(int pageId) throws IOException {
        if (nodeCache.containsKey(pageId)) {
            return nodeCache.get(pageId);
        }

        Page page;
        if (useCache) {
            page = bufferPool.fetchPage(pageId);
        } else {
            page = diskManager.readPage(pageId);
        }

        byte[] data = page.getData();
        ByteBuffer buffer = ByteBuffer.wrap(data);
        buffer.position(Page.HEADER_SIZE);

        int dataLength = buffer.remaining();
        byte[] nodeData = new byte[dataLength];
        buffer.get(nodeData);

        LeafNode<K, V> node = (LeafNode<K, V>) NodeSerializer.deserializeLeafNode(
                nodeData, order);

        nodeCache.put(pageId, node);
        nodeToPageId.put(node, pageId);

        // Don't unpin yet - keep it pinned while in nodeCache

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

        // FIXED: Always write through DiskManager to update numPages
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

    public String getBufferPoolStats() {
        if (bufferPool != null) {
            return bufferPool.getStatistics();
        }
        return "BufferPool: DISABLED";
    }

    public String getStatistics() {
        String diskStats = diskManager.toString();
        String bufferStats = getBufferPoolStats();
        return diskStats + "\n" + bufferStats;
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }

        closed = true;

        if (bufferPool != null) {
            bufferPool.flushAll();
        }

        diskManager.flush();
        diskManager.close();

        logger.info("Closed DiskBPlusTree");
    }
}
