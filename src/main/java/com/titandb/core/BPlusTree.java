package com.titandb.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * B+ Tree implementation for TitanDB.
 *
 * Features:
 * - Order-preserving key-value storage
 * - O(log n) insert, search, delete
 * - Efficient range scans via linked leaf nodes
 * - Automatic rebalancing (split/merge)
 *
 * Properties:
 * - All data stored in leaf nodes
 * - Internal nodes only for routing
 * - All leaves at same depth
 * - Nodes at least 50% full (except root)
 *
 * @param <K> Key type (must be Comparable)
 * @param <V> Value type
 */
public class BPlusTree<K extends Comparable<K>, V> {
    private static final Logger logger = LoggerFactory.getLogger(BPlusTree.class);

    private final int order;      // Maximum children per internal node
    private Node<K, V> root;      // Root node (can be internal or leaf)
    private int size;             // Total number of key-value pairs

    /**
     * Constructor with specified order.
     *
     * @param order Maximum number of children per internal node.
     *              For learning, use order=4. Production: order=100-200.
     */
    public BPlusTree(int order) {
        if (order < 3) {
            throw new IllegalArgumentException("Order must be at least 3");
        }
        this.order = order;
        this.root = new LeafNode<>(order);
        this.size = 0;
        logger.debug("Created B+ tree with order {}", order);
    }

    /**
     * Default constructor with order 4 (good for learning/testing).
     */
    public BPlusTree() {
        this(4);
    }

    /**
     * Get the current number of key-value pairs in the tree.
     */
    public int size() {
        return size;
    }

    /**
     * Check if the tree is empty.
     */
    public boolean isEmpty() {
        return size == 0;
    }

    /**
     * Get the root node (for testing/visualization).
     */
    public Node<K, V> getRoot() {
        return root;
    }

    /**
     * Search for a value by key.
     *
     * Algorithm:
     * 1. Start at root
     * 2. If internal node, use keys to route to correct child
     * 3. Repeat until reaching leaf
     * 4. Search within leaf node
     *
     * Time complexity: O(log n)
     *
     * @param key The key to search for
     * @return The value if found, null otherwise
     */
    public V search(K key) {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }

        logger.debug("Searching for key: {}", key);
        LeafNode<K, V> leaf = findLeafNode(key);
        V value = leaf.search(key);

        logger.debug("Search result for key {}: {}", key, value != null ? "found" : "not found");
        return value;
    }

    /**
     * Find the leaf node that should contain the given key.
     * Helper method used by insert, search, delete.
     *
     * @param key The key to search for
     * @return The leaf node where the key should be
     */
    private LeafNode<K, V> findLeafNode(K key) {
        Node<K, V> current = root;

        // Traverse down the tree until we reach a leaf
        while (!current.isLeaf()) {
            InternalNode<K, V> internal = (InternalNode<K, V>) current;
            int childIndex = internal.findChildIndex(key);
            current = internal.getChild(childIndex);
        }

        return (LeafNode<K, V>) current;
    }

    /**
     * Insert a key-value pair into the tree.
     *
     * Algorithm:
     * 1. Find the leaf node where key should go
     * 2. Insert into leaf
     * 3. If leaf overflows (size > order), split it
     * 4. Propagate split up to parent (may cause parent to split)
     * 5. If root splits, create new root (tree grows taller)
     *
     * Time complexity: O(log n)
     *
     * @param key The key to insert
     * @param value The value to insert
     * @return true if inserted (new key), false if updated (existing key)
     */
    public boolean insert(K key, V value) {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }

        logger.debug("Inserting key: {}, value: {}", key, value);

        // Find the leaf where this key should go
        LeafNode<K, V> leaf = findLeafNode(key);
        boolean isNewKey = leaf.insert(key, value);

        if (isNewKey) {
            size++;
        }

        // Check if leaf overflowed
        if (leaf.isFull()) {
            logger.debug("Leaf node is full, splitting...");
            splitLeafNode(leaf);
        }

        return isNewKey;
    }

    /**
     * Split a full leaf node and propagate changes up the tree.
     *
     * Algorithm:
     * 1. Split leaf into two
     * 2. Get the first key of the new (right) leaf
     * 3. Insert this key into parent (to route to new leaf)
     * 4. If parent doesn't exist (leaf was root), create new root
     * 5. If parent overflows, recursively split parent
     *
     * @param leaf The leaf node to split
     */
    private void splitLeafNode(LeafNode<K, V> leaf) {
        LeafNode<K, V> newLeaf = leaf.split();
        K firstKeyOfNewLeaf = newLeaf.getKey(0);

        logger.debug("Split leaf. First key of new leaf: {}", firstKeyOfNewLeaf);

        // If splitting root, create new root
        if (leaf.parent == null) {
            InternalNode<K, V> newRoot = new InternalNode<>(order);
            newRoot.appendChild(leaf);
            newRoot.keys.add(firstKeyOfNewLeaf);
            newRoot.appendChild(newLeaf);

            leaf.parent = newRoot;
            newLeaf.parent = newRoot;
            root = newRoot;

            logger.debug("Created new root. Tree height increased.");
        } else {
            // Insert new leaf into existing parent
            InternalNode<K, V> parent = (InternalNode<K, V>) leaf.parent;
            parent.insertChild(firstKeyOfNewLeaf, newLeaf);

            // Check if parent overflowed
            if (parent.isFull()) {
                logger.debug("Parent node is full, splitting...");
                splitInternalNode(parent);
            }
        }
    }

    /**
     * Split a full internal node and propagate changes up the tree.
     *
     * Algorithm:
     * 1. Split internal node
     * 2. Get the pushed-up key (middle key)
     * 3. Insert pushed-up key into parent
     * 4. If parent doesn't exist (internal was root), create new root
     * 5. If parent overflows, recursively split parent
     *
     * @param internal The internal node to split
     */
    private void splitInternalNode(InternalNode<K, V> internal) {
        InternalNode.SplitResult<K, V> result = internal.split();
        K pushedUpKey = result.pushedUpKey;
        InternalNode<K, V> newInternal = result.newNode;

        logger.debug("Split internal node. Pushed up key: {}", pushedUpKey);

        // If splitting root, create new root
        if (internal.parent == null) {
            InternalNode<K, V> newRoot = new InternalNode<>(order);
            newRoot.appendChild(internal);
            newRoot.keys.add(pushedUpKey);
            newRoot.appendChild(newInternal);

            internal.parent = newRoot;
            newInternal.parent = newRoot;
            root = newRoot;

            logger.debug("Created new root. Tree height increased.");
        } else {
            // Insert into existing parent
            InternalNode<K, V> parent = (InternalNode<K, V>) internal.parent;
            parent.insertChild(pushedUpKey, newInternal);

            // Check if parent overflowed (recursive split)
            if (parent.isFull()) {
                logger.debug("Grandparent node is full, splitting recursively...");
                splitInternalNode(parent);
            }
        }
    }

    /**
     * Delete a key-value pair from the tree.
     *
     * NOTE: This is a simplified version that doesn't handle merging/borrowing.
     * Full implementation will be added in Phase 1 completion.
     *
     * @param key The key to delete
     * @return The deleted value, or null if key not found
     */
    public V delete(K key) {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }

        logger.debug("Deleting key: {}", key);

        LeafNode<K, V> leaf = findLeafNode(key);
        V deletedValue = leaf.delete(key);

        if (deletedValue != null) {
            size--;
            logger.debug("Deleted key: {}", key);

            // TODO: Handle underfull nodes (merge/borrow)
            // For now, we allow nodes to be less than 50% full
        }

        return deletedValue;
    }

    /**
     * Perform a range scan from startKey (inclusive) to endKey (exclusive).
     * Returns all key-value pairs in the range.
     *
     * Algorithm:
     * 1. Find leaf containing startKey
     * 2. Walk through linked leaves, collecting entries
     * 3. Stop when reaching endKey or end of tree
     *
     * Time complexity: O(log n + m), where m = number of results
     *
     * @param startKey Start of range (inclusive)
     * @param endKey End of range (exclusive)
     * @return List of entries in the range
     */
    public List<Entry<K, V>> rangeScan(K startKey, K endKey) {
        if (startKey == null || endKey == null) {
            throw new IllegalArgumentException("Keys cannot be null");
        }
        if (startKey.compareTo(endKey) >= 0) {
            throw new IllegalArgumentException("startKey must be less than endKey");
        }

        logger.debug("Range scan: [{}, {})", startKey, endKey);

        List<Entry<K, V>> results = new ArrayList<>();
        LeafNode<K, V> leaf = findLeafNode(startKey);

        // Walk through linked leaves
        while (leaf != null) {
            for (int i = 0; i < leaf.getKeyCount(); i++) {
                K key = leaf.getKey(i);

                // Skip keys before startKey
                if (key.compareTo(startKey) < 0) {
                    continue;
                }

                // Stop if we've reached endKey
                if (key.compareTo(endKey) >= 0) {
                    logger.debug("Range scan found {} entries", results.size());
                    return results;
                }

                // Add entry to results
                results.add(new Entry<>(key, leaf.getValue(i)));
            }

            // Move to next leaf
            leaf = leaf.getNext();
        }

        logger.debug("Range scan found {} entries", results.size());
        return results;
    }

    /**
     * Get all entries in the tree (full scan).
     * Useful for debugging and testing.
     *
     * @return List of all entries in sorted order
     */
    public List<Entry<K, V>> getAllEntries() {
        List<Entry<K, V>> results = new ArrayList<>();

        // Find leftmost leaf
        Node<K, V> current = root;
        while (!current.isLeaf()) {
            InternalNode<K, V> internal = (InternalNode<K, V>) current;
            current = internal.getChild(0);
        }

        // Walk through all leaves
        LeafNode<K, V> leaf = (LeafNode<K, V>) current;
        while (leaf != null) {
            for (int i = 0; i < leaf.getKeyCount(); i++) {
                results.add(new Entry<>(leaf.getKey(i), leaf.getValue(i)));
            }
            leaf = leaf.getNext();
        }

        return results;
    }

    /**
     * Print the tree structure (for debugging/visualization).
     * Shows the tree level by level.
     */
    public void printTree() {
        if (root == null) {
            System.out.println("Empty tree");
            return;
        }

        System.out.println("\n===== B+ Tree Structure =====");
        printNode(root, 0);
        System.out.println("=============================\n");
    }

    /**
     * Recursive helper to print tree structure.
     */
    private void printNode(Node<K, V> node, int level) {
        String indent = "  ".repeat(level);
        System.out.println(indent + "Level " + level + ": " + node);

        if (!node.isLeaf()) {
            InternalNode<K, V> internal = (InternalNode<K, V>) node;
            for (Node<K, V> child : internal.getChildren()) {
                printNode(child, level + 1);
            }
        }
    }
}
