package com.titandb.core;

import java.util.ArrayList;
import java.util.List;

/**
 * Leaf node in the B+ tree.
 * Stores actual key-value pairs (entries).
 * All leaf nodes are linked together for efficient range scans.
 * Structure: [Entry1, Entry2, ..., EntryN] → next LeafNode
 *
 * @param <K> Key type
 * @param <V> Value type
 */
public class LeafNode<K extends Comparable<K>, V> extends Node<K, V> {
    private List<V> values;         // Values corresponding to keys (parallel array)
    protected LeafNode<K, V> next;    // Pointer to next leaf (for range scans)
    private LeafNode<K, V> previous; // Pointer to previous leaf (for reverse scans)

    /**
     * Constructor for LeafNode.
     *
     * @param order Maximum number of entries this leaf can hold
     */
    public LeafNode(int order) {
        super(order);
        this.values = new ArrayList<>();
        this.next = null;
        this.previous = null;
    }

    @Override
    public boolean isLeaf() {
        return true;
    }

    /**
     * Insert a key-value pair into this leaf node.
     * Maintains sorted order by key.
     *
     * @param key The key to insert
     * @param value The value to insert
     * @return true if inserted successfully, false if key already exists
     */
    public boolean insert(K key, V value) {
        int index = binarySearch(key);

        if (index >= 0) {
            // Key already exists, update the value
            values.set(index, value);
            return false; // Didn't insert, just updated
        } else {
            // Key doesn't exist, insert at correct position
            int insertionPoint = -(index + 1);
            keys.add(insertionPoint, key);
            values.add(insertionPoint, value);
            return true;
        }
    }

    /**
     * Search for a value by key.
     *
     * @param key The key to search
     * @return The value if found, null otherwise
     */
    public V search(K key) {
        int index = binarySearch(key);
        if (index >= 0) {
            return values.get(index);
        }
        return null;
    }

    /**
     * Delete a key-value pair from this leaf.
     *
     * @param key The key to delete
     * @return The deleted value, or null if key not found
     */
    public V delete(K key) {
        int index = binarySearch(key);
        if (index >= 0) {
            keys.remove(index);
            return values.remove(index);
        }
        return null;
    }

    /**
     * Get value at specific index.
     */
    public V getValue(int index) {
        return values.get(index);
    }

    /**
     * Get all values (for testing).
     */
    public List<V> getValues() {
        return new ArrayList<>(values);
    }

    /**
     * Get next leaf node in the linked list.
     */
    public LeafNode<K, V> getNext() {
        return next;
    }

    /**
     * Set next leaf node.
     */
    public void setNext(LeafNode<K, V> next) {
        this.next = next;
    }

    /**
     * Get previous leaf node.
     */
    public LeafNode<K, V> getPrevious() {
        return previous;
    }

    /**
     * Set previous leaf node.
     */
    public void setPrevious(LeafNode<K, V> previous) {
        this.previous = previous;
    }

    /**
     * Split this leaf into two leaves when it overflows.
     * This leaf keeps the first half, returns a new leaf with second half.
     *
     * Algorithm:
     * 1. Create new leaf
     * 2. Move second half of entries to new leaf
     * 3. Update linked list pointers
     * 4. Return new leaf (caller will handle parent update)
     *
     * @return The new leaf node created from the split
     */
    /**
     * Split this leaf node when full.
     *
     * @return New right sibling node with half the keys
     */
    /**
     * Split this full leaf node.
     * Returns the new right sibling and the key to promote to parent.
     */
    public LeafNode<K, V> split() {
        int mid = keys.size() / 2;

        // Create new right sibling
        LeafNode<K, V> rightSibling = new LeafNode<>(order);

        // Move right half to new node
        for (int i = mid; i < keys.size(); i++) {
            rightSibling.keys.add(keys.get(i));
            rightSibling.values.add(values.get(i));
        }

        // Remove moved keys from this node
        keys.subList(mid, keys.size()).clear();
        values.subList(mid, values.size()).clear();

        // Link the nodes (maintain linked list)
        rightSibling.next = this.next;
        this.next = rightSibling;

        return rightSibling;
    }

    /**
     * Get the smallest key (for parent routing).
     */
    public K getSmallestKey() {
        return keys.isEmpty() ? null : keys.get(0);
    }


    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("LeafNode[");
        for (int i = 0; i < keys.size(); i++) {
            sb.append(keys.get(i)).append(":").append(values.get(i));
            if (i < keys.size() - 1) {
                sb.append(", ");
            }
        }
        sb.append("]");
        return sb.toString();
    }
}
