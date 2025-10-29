package com.titandb.core;

import java.util.ArrayList;
import java.util.List;

/**
 * Abstract base class for all B+ tree nodes.
 * Provides common functionality for InternalNode and LeafNode.
 *
 * @param <K> Key type
 * @param <V> Value type
 */
public abstract class Node<K extends Comparable<K>, V> {
    protected final int order; // Maximum number of children (for internal) or entries (for leaf)
    protected List<K> keys;    // Keys in this node (always sorted)
    protected Node<K, V> parent; // Parent node (null for root)

    /**
     * Constructor for Node.
     *
     * @param order Maximum capacity of this node
     */
    public Node(int order) {
        this.order = order;
        this.keys = new ArrayList<>();
        this.parent = null;
    }

    /**
     * Check if this node is a leaf node.
     * Leaf nodes store actual key-value pairs.
     * Internal nodes only store keys and pointers.
     */
    public abstract boolean isLeaf();

    /**
     * Check if this node is full (needs splitting on next insert).
     */
    public boolean isFull() {
        return keys.size() >= order;
    }

    /**
     * Check if this node is underfull (needs merging/borrowing).
     * Nodes must be at least 50% full (except root).
     */
    public boolean isUnderfull() {
        // Root can have as few as 1 key
        if (parent == null) {
            return false;
        }
        // Other nodes must have at least ceil(order/2) - 1 keys
        return keys.size() < (int) Math.ceil(order / 2.0) - 1;
    }

    /**
     * Get the number of keys in this node.
     */
    public int getKeyCount() {
        return keys.size();
    }

    /**
     * Get key at specific index.
     */
    public K getKey(int index) {
        return keys.get(index);
    }

    /**
     * Get all keys (for testing/debugging).
     */
    public List<K> getKeys() {
        return new ArrayList<>(keys);
    }

    /**
     * Binary search for the index where key should be inserted.
     * Returns the index of the key if found, or -(insertion_point + 1) if not found.
     *
     * This is the same as Collections.binarySearch().
     */
    protected int binarySearch(K key) {
        int low = 0;
        int high = keys.size() - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1; // Unsigned right shift (avoids overflow)
            K midKey = keys.get(mid);
            int cmp = midKey.compareTo(key);

            if (cmp < 0) {
                low = mid + 1;
            } else if (cmp > 0) {
                high = mid - 1;
            } else {
                return mid; // Key found
            }
        }
        return -(low + 1); // Key not found, return insertion point
    }

    /**
     * Add a key directly to the keys list.
     * Used during deserialization - bypasses normal insertion logic.
     *
     * @param key The key to add
     */
    public void addKey(K key) {
        keys.add(key);
    }

    /**
     * Get all keys as a new list.
     * Used during serialization.
     *
     * @return Copy of the keys list
     */
    public List<K> getAllKeys() {
        return new ArrayList<>(keys);
    }

    @Override
    public String toString() {
        return keys.toString();
    }
}