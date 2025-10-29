package com.titandb.core;

import java.util.Objects;

/**
 * Represents a key-value entry in the B+ tree.
 * Implements Comparable to maintain sorted order.
 *
 * @param <K> Key type (must be Comparable)
 * @param <V> Value type
 */
public class  Entry<K extends Comparable<K>, V> implements Comparable<Entry<K, V>> {


    private final K key;
    private V value;

    public Entry(K key, V value) {
        this.key = Objects.requireNonNull(key, "Key cannot be null");
        this.value = value;
    }

    public K getKey() {
        return key;
    }

    public V getValue() {
        return value;
    }

    public void setValue(V value) {
        this.value = value;
    }
    /**
     * Compare entries based on keys for sorting.
     * This enables binary search within nodes.
     */
    @Override
    public int compareTo(Entry<K, V> other) {
        return this.key.compareTo(other.key);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Entry)) return false;
        Entry<?, ?> entry = (Entry<?, ?>) o;
        return key.equals(entry.key);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key);
    }

    @Override
    public String toString() {
        return key + ":" + value;
    }
}
