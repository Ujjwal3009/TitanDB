package com.titandb.core;

import com.titandb.concurrency.*;
import com.titandb.storage.DiskManager;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * MVCC + Disk-Based B+ Tree
 *
 * Combines:
 * ✅ DiskBPlusTree for persistence
 * ✅ MVCC for concurrency
 * ✅ In-memory version tracking
 */
public class ConcurrentDiskBPlusTree<K extends Comparable<K>, V> {

    private final DiskBPlusTree<K, V> diskTree;
    private final TransactionManager txnManager;

    // Version tracking: key -> {txnId -> value}
    private final Map<K, Map<Long, V>> versionMap;

    public ConcurrentDiskBPlusTree(String dbPath, int order) throws IOException {
        this.diskTree = new DiskBPlusTree<>(dbPath, order);
        this.txnManager = new TransactionManager();
        this.versionMap = new HashMap<>();
    }

    /**
     * Begin a transaction
     */
    public TransactionManager.Transaction begin() {
        return txnManager.begin();
    }

    /**
     * Insert with transaction support
     */
    public void insert(TransactionManager.Transaction txn, K key, V value)
            throws IOException {

        // Track version in memory
        versionMap.computeIfAbsent(key, k -> new HashMap<>())
                .put(txn.txnId, value);

        System.out.println("✅ Txn " + txn.txnId + " inserted: " + key);
    }

    /**
     * Search with TRUE snapshot isolation!
     */
    public V search(TransactionManager.Transaction txn, K key) throws IOException {
        Map<Long, V> versions = versionMap.get(key);

        if (versions == null) {
            return diskTree.search(key);
        }

        // Sort versions by transaction ID (newest first)
        java.util.List<Long> sortedTxns = new java.util.ArrayList<>(versions.keySet());
        sortedTxns.sort((a, b) -> Long.compare(b, a));

        // Find most recent version THIS TRANSACTION CAN SEE
        for (long createdByTxn : sortedTxns) {

            // Rule: Can only see if:
            // 1. Created by THIS transaction, OR
            // 2. Created by committed transaction that started BEFORE us

            if (createdByTxn == txn.txnId) {
                // Our own write - always visible
                V result = versions.get(createdByTxn);
                System.out.println("👁️ Txn " + txn.txnId + " read OWN: " + key + " = " + result);
                return result;
            }

            // Check if it's from another txn
            if (txnManager.isRunning(createdByTxn)) {
                // Still running - can't see it!
                continue;
            }

            if (txnManager.isCommitted(createdByTxn)) {
                // Committed - but did it start BEFORE us?
                if (createdByTxn < txn.startLSN) {
                    // YES! We can see it (it started before us)
                    V result = versions.get(createdByTxn);
                    System.out.println("👁️ Txn " + txn.txnId + " read: " + key
                            + " = " + result + " (from earlier Txn " + createdByTxn + ")");
                    return result;
                }
            }
        }

        return null;
    }


    /**
     * Commit - persist to disk
     */
    public void commit(TransactionManager.Transaction txn) throws IOException {
        // Mark as committed in MVCC
        txnManager.commit(txn.txnId);

        // Write all versions from this transaction to disk
        for (Map.Entry<K, Map<Long, V>> entry : versionMap.entrySet()) {
            Map<Long, V> versions = entry.getValue();

            if (versions.containsKey(txn.txnId)) {
                V value = versions.get(txn.txnId);
                diskTree.insert(entry.getKey(), value);
                System.out.println("💾 Persisted Txn " + txn.txnId + ": " + entry.getKey());
            }
        }

        System.out.println("✅ Committed Txn " + txn.txnId + " (persisted!)");
    }

    /**
     * Abort transaction
     */
    public void abort(TransactionManager.Transaction txn) throws IOException {
        txnManager.abort(txn.txnId);

        // Remove uncommitted versions
        for (Map<Long, V> versions : versionMap.values()) {
            versions.remove(txn.txnId);
        }

        System.out.println("❌ Aborted Txn " + txn.txnId);
    }

    /**
     * Close resources
     */
    public void close() throws IOException {
        diskTree.close();
    }

    public String getStatistics() {
        return diskTree.getStatistics();
    }
}
