package com.titandb.core;

import java.util.ArrayList;
import java.util.List;

/**
 * Internal node in the B+ tree.
 * Stores keys (for routing) and pointers to child nodes.
 * Does NOT store actual values - only guides search to correct leaf.
 *
 * Property: If node has n keys, it must have (n+1) children.
 * Example: keys=[20, 40], children=[P0, P1, P2]
 *   - P0: contains keys < 20
 *   - P1: contains keys 20 ≤ k < 40
 *   - P2: contains keys ≥ 40
 *
 * @param <K> Key type
 * @param <V> Value type (not stored here, but needed for type consistency)
 */
public class InternalNode<K extends Comparable<K>, V> extends Node<K, V> {
    private List<Node<K, V>> children; // Pointers to child nodes (always size = keys.size() + 1)

    /**
     * Constructor for InternalNode.
     *
     * @param order Maximum number of children this node can have
     */
    public InternalNode(int order) {
        super(order);
        this.children = new ArrayList<>();
    }

    @Override
    public boolean isLeaf() {
        return false;
    }

    /**
     * Get child at specific index.
     */
    public Node<K, V> getChild(int index) {
        return children.get(index);
    }

    /**
     * Get all children (for testing/debugging).
     */
    public List<Node<K, V>> getChildren() {
        return new ArrayList<>(children);
    }

    /**
     * Get number of children.
     */
    public int getChildCount() {
        return children.size();
    }

    /**
     * Find the index of the child that should contain the given key.
     *
     * Algorithm:
     * - If key < keys[0], return 0 (leftmost child)
     * - If keys[i-1] ≤ key < keys[i], return i
     * - If key ≥ keys[last], return children.size()-1 (rightmost child)
     *
     * @param key The key to search for
     * @return Index of the child to traverse
     */
    public int findChildIndex(K key) {
        int index = binarySearch(key);

        if (index >= 0) {
            // Exact match found, go to right child
            // (keys at index go to child at index+1)
            return index + 1;
        } else {
            // Key not found, insertion point tells us which child
            int insertionPoint = -(index + 1);
            return insertionPoint;
        }
    }

    /**
     * Insert a key and corresponding child pointer into this internal node.
     * Maintains sorted order of keys.
     *
     * Important: The new child will be inserted AFTER the key.
     * Example: insertChild(25, newChild) with keys=[10, 30]
     *   Before: keys=[10, 30], children=[C0, C1, C2]
     *   After:  keys=[10, 25, 30], children=[C0, C1, newChild, C2]
     *
     * @param key The key to insert
     * @param child The child node to insert
     */
    public void insertChild(K key, Node<K, V> child) {
        int index = binarySearch(key);
        int insertionPoint;

        if (index >= 0) {
            // Key already exists (shouldn't happen in B+ tree, but handle it)
            insertionPoint = index + 1;
        } else {
            insertionPoint = -(index + 1);
        }

        // Insert key at correct position
        keys.add(insertionPoint, key);
        // Insert child AFTER the key
        children.add(insertionPoint + 1, child);
        child.parent = this;
    }

    /**
     * Replace the child at given index with a new child.
     * Used when a child is split.
     *
     * @param index Index of the child to replace
     * @param newChild The new child node
     */
    public void setChild(int index, Node<K, V> newChild) {
        children.set(index, newChild);
        newChild.parent = this;
    }

    /**
     * Add a child at the end (used during initial tree construction).
     *
     * @param child The child to append
     */
    public void appendChild(Node<K, V> child) {
        children.add(child);
        child.parent = this;
    }

    /**
     * Split this internal node into two when it overflows.
     * Similar to leaf split, but we're splitting keys + children.
     *
     * Algorithm:
     * 1. Create new internal node
     * 2. Move second half of keys/children to new node
     * 3. The middle key is "pushed up" to parent (not copied)
     * 4. Return the pushed-up key and new node
     *
     * Example: keys=[10, 20, 30, 40] (overflow for order=4)
     *   After split:
     *   - This node: keys=[10, 20]
     *   - New node: keys=[40]
     *   - Pushed up: key=30
     *
     * @return A SplitResult containing the pushed-up key and new sibling
     */
    public SplitResult<K, V> split() {
        int midpoint = keys.size() / 2;
        K pushedUpKey = keys.get(midpoint);

        // Create new internal node for second half
        InternalNode<K, V> newInternal = new InternalNode<>(order);
        newInternal.parent = this.parent;

        // Move second half of keys to new node (excluding pushed-up key)
        newInternal.keys.addAll(keys.subList(midpoint + 1, keys.size()));
        // Move second half of children to new node
        newInternal.children.addAll(children.subList(midpoint + 1, children.size()));

        // Update parent pointers of moved children
        for (Node<K, V> child : newInternal.children) {
            child.parent = newInternal;
        }

        // Remove moved entries from this node
        keys.subList(midpoint, keys.size()).clear();
        children.subList(midpoint + 1, children.size()).clear();

        return new SplitResult<>(pushedUpKey, newInternal);
    }

    /**
     * Helper class to return results from internal node split.
     * Contains the key to push up to parent and the new sibling node.
     */
    public static class SplitResult<K extends Comparable<K>, V> {
        public final K pushedUpKey;
        public final InternalNode<K, V> newNode;

        public SplitResult(K pushedUpKey, InternalNode<K, V> newNode) {
            this.pushedUpKey = pushedUpKey;
            this.newNode = newNode;
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("InternalNode[keys=").append(keys)
                .append(", children=").append(children.size()).append("]");
        return sb.toString();
    }
}
