package com.titandb;

import com.titandb.core.BPlusTree;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TitanDB Demo Application.
 * Demonstrates basic usage of the B+ Tree storage engine.
 */
public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        logger.info("🗄️  TitanDB v0.1.0-alpha - In-Memory B+ Tree Demo");

        // Create a B+ tree
        BPlusTree<Integer, String> db = new BPlusTree<>(4);

        // Insert some data
        logger.info("Inserting sample data...");
        db.insert(10, "Alice");
        db.insert(20, "Bob");
        db.insert(30, "Charlie");
        db.insert(40, "Dave");
        db.insert(50, "Eve");

        // Search
        logger.info("Searching for key 30: {}", db.search(30));

        // Range scan
        logger.info("Range scan [20, 50):");
        db.rangeScan(20, 50).forEach(entry ->
                logger.info("  {} -> {}", entry.getKey(), entry.getValue())
        );

        // Print tree structure
        db.printTree();

        logger.info("✅ Demo completed successfully!");
        logger.info("Total keys: {}", db.size());
    }
}
