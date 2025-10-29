package com.titandb.benchmark;

import com.titandb.core.DiskBPlusTree;
import com.titandb.storage.DiskManager;

import java.io.IOException;
import java.util.Random;

public class SimpleBenchmark {

    // FIXED: Reduced operations and increased order
    private static final int OPERATIONS = 100;  // Reduced from 1000
    private static final int TREE_ORDER = 100;   // Increased from 4
    private static final String BENCH_NO_CACHE = "bench_nocache.titandb";
    private static final String BENCH_WITH_CACHE = "bench_cache.titandb";

    public static void main(String[] args) throws Exception {
        System.out.println("═".repeat(70));
        System.out.println("  TitanDB Week 3: Buffer Pool Performance Benchmark");
        System.out.println("  Operations: " + OPERATIONS + " | Tree Order: " + TREE_ORDER);
        System.out.println("═".repeat(70));
        System.out.println();

        long timeNoCache = benchmarkWithoutCache();
        System.out.println();
        long timeWithCache = benchmarkWithCache();

        System.out.println();
        System.out.println("═".repeat(70));
        System.out.println("  RESULTS COMPARISON");
        System.out.println("═".repeat(70));

        double speedup = (double) timeNoCache / timeWithCache;

        System.out.printf("Without Cache: %,d ms%n", timeNoCache / 1_000_000);
        System.out.printf("With Cache:    %,d ms%n", timeWithCache / 1_000_000);
        System.out.printf("%nSpeedup:       %.2fx FASTER! 🚀%n", speedup);

        if (speedup >= 5) {
            System.out.println("\n✅ TARGET ACHIEVED: 5x+ speedup!");
        } else if (speedup >= 3) {
            System.out.println("\n⚠️  Good progress: 3-5x speedup");
        } else {
            System.out.println("\n❌ Cache not effective enough");
        }

        System.out.println("\n💡 Note: Using order=" + TREE_ORDER +
                " to fit " + OPERATIONS + " keys in single page");
        System.out.println("💡 Real B+ trees split nodes when full (Week 4 feature)");
        System.out.println("\n" + "═".repeat(70));
    }

    private static long benchmarkWithoutCache() throws IOException {
        System.out.println("🐢 Testing WITHOUT Buffer Pool (Direct Disk I/O)");
        System.out.println("─".repeat(70));

        if (DiskManager.databaseExists(BENCH_NO_CACHE)) {
            DiskManager.deleteDatabase(BENCH_NO_CACHE);
        }

        DiskBPlusTree<Integer, String> tree = new DiskBPlusTree<>(
                BENCH_NO_CACHE, TREE_ORDER, false);  // Cache OFF

        // Insert phase
        long insertStart = System.nanoTime();
        for (int i = 0; i < OPERATIONS; i++) {
            tree.insert(i, "Value_" + i);
        }
        long insertTime = System.nanoTime() - insertStart;

        System.out.printf("Insert %,d ops: %,d ms (%.2f ops/ms)%n",
                OPERATIONS, insertTime / 1_000_000,
                OPERATIONS / (insertTime / 1_000_000.0));

        // Search phase
        Random random = new Random(42);
        long searchStart = System.nanoTime();
        for (int i = 0; i < OPERATIONS; i++) {
            tree.search(random.nextInt(OPERATIONS));
        }
        long searchTime = System.nanoTime() - searchStart;

        System.out.printf("Search %,d ops: %,d ms (%.2f ops/ms)%n",
                OPERATIONS, searchTime / 1_000_000,
                OPERATIONS / (searchTime / 1_000_000.0));

        long totalTime = insertTime + searchTime;
        System.out.printf("Total Time: %,d ms%n", totalTime / 1_000_000);
        System.out.println("\nDisk I/O Stats:");
        System.out.println(tree.getStatistics());

        tree.close();
        DiskManager.deleteDatabase(BENCH_NO_CACHE);

        return totalTime;
    }

    private static long benchmarkWithCache() throws IOException {
        System.out.println("🚀 Testing WITH Buffer Pool (Cached)");
        System.out.println("─".repeat(70));

        if (DiskManager.databaseExists(BENCH_WITH_CACHE)) {
            DiskManager.deleteDatabase(BENCH_WITH_CACHE);
        }

        DiskBPlusTree<Integer, String> tree = new DiskBPlusTree<>(
                BENCH_WITH_CACHE, TREE_ORDER, true);  // Cache ON

        // Insert phase
        long insertStart = System.nanoTime();
        for (int i = 0; i < OPERATIONS; i++) {
            tree.insert(i, "Value_" + i);
        }
        long insertTime = System.nanoTime() - insertStart;

        System.out.printf("Insert %,d ops: %,d ms (%.2f ops/ms)%n",
                OPERATIONS, insertTime / 1_000_000,
                OPERATIONS / (insertTime / 1_000_000.0));

        // Search phase
        Random random = new Random(42);
        long searchStart = System.nanoTime();
        for (int i = 0; i < OPERATIONS; i++) {
            tree.search(random.nextInt(OPERATIONS));
        }
        long searchTime = System.nanoTime() - searchStart;

        System.out.printf("Search %,d ops: %,d ms (%.2f ops/ms)%n",
                OPERATIONS, searchTime / 1_000_000,
                OPERATIONS / (searchTime / 1_000_000.0));

        long totalTime = insertTime + searchTime;
        System.out.printf("Total Time: %,d ms%n", totalTime / 1_000_000);
        System.out.println("\nBuffer Pool Stats:");
        System.out.println(tree.getStatistics());

        tree.close();
        DiskManager.deleteDatabase(BENCH_WITH_CACHE);

        return totalTime;
    }
}
