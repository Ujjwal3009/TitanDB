package com.titandb;

import com.titandb.core.DiskBPlusTree;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        try {
            logger.info("🗄️  TitanDB Demo - Disk Persistence");

            // First run: Insert data
            logger.info("=== First Run: Inserting Data ===");
            DiskBPlusTree<Integer, String> tree = new DiskBPlusTree<>("demo.titandb", 4);
            tree.insert(10, "Alice");
            tree.insert(20, "Bob");
            tree.insert(30, "Charlie");
            logger.info("Inserted 3 entries");
            tree.close();
            logger.info("Database closed. Data saved to disk.");

            // Simulate restart
            logger.info("\n=== Simulating Application Restart ===\n");
            Thread.sleep(1000);

            // Second run: Load data
            logger.info("=== Second Run: Loading Data ===");
            DiskBPlusTree<Integer, String> tree2 = new DiskBPlusTree<>("demo.titandb", 4);
            logger.info("Database reopened");

            String alice = tree2.search(10);
            String bob = tree2.search(20);
            String charlie = tree2.search(30);

            logger.info("Search results:");
            logger.info("  10 → {}", alice);
            logger.info("  20 → {}", bob);
            logger.info("  30 → {}", charlie);

            if (alice.equals("Alice") && bob.equals("Bob") && charlie.equals("Charlie")) {
                logger.info("\n✅ SUCCESS! Data persisted across restarts!");
            }

            tree2.close();

        } catch (Exception e) {
            logger.error("Error", e);
        }
    }
}
