package com.titandb.benchmark;

import com.titandb.core.DiskBPlusTree;
import com.titandb.storage.DiskManager;

import java.io.IOException;
import java.util.Random;

/**
 * ULTIMATE benchmark showing massive cache benefits.
 *
 * Key: Write once, read many times (80/20 rule)
 */
public class SimpleBenchmark {

    private static final int INSERT_OPS = 200;
    private static final int SEARCH_OPS = 5000;
    private static final int TREE_ORDER = 100;


    private static final String BENCH_NO_CACHE = "bench_nocache.titandb";
    private static final String BENCH_WITH_CACHE = "bench_cache.titandb";

    public static void main(String[] args) throws Exception {
        System.out.println("═".repeat(70));
        System.out.println("  TitanDB: Read-Heavy Workload Benchmark");
        System.out.println("  Inserts: " + INSERT_OPS);
        System.out.println("  Searches: " + SEARCH_OPS + " (10x more reads than writes)");
        System.out.println("═".repeat(70));
        System.out.println();

        long timeNoCache = benchmarkWithoutCache();
        System.out.println();
        long timeWithCache = benchmarkWithCache();

        System.out.println();
        System.out.println("═".repeat(70));
        System.out.println("  🎯 FINAL RESULTS");
        System.out.println("═".repeat(70));

        double speedup = (double) timeNoCache / timeWithCache;

        System.out.printf("Without Cache: %,d ms%n", timeNoCache / 1_000_000);
        System.out.printf("With Cache:    %,d ms%n", timeWithCache / 1_000_000);
        System.out.printf("%nSpeedup:       %.2fx FASTER! 🚀%n", speedup);

        if (speedup >= 5) {
            System.out.println("\n🏆 OUTSTANDING: 5x+ speedup! This is production-grade!");
        } else if (speedup >= 3) {
            System.out.println("\n✅ EXCELLENT: 3-5x speedup achieved!");
        } else if (speedup >= 2) {
            System.out.println("\n✅ GOOD: 2-3x speedup");
        } else {
            System.out.println("\n⚠️  Something wrong - cache should help more!");
        }

        System.out.println("\n💡 Real databases have 80/20 read/write ratio");
        System.out.println("💡 Buffer pool shines on read-heavy workloads!");
        System.out.println("\n" + "═".repeat(70));
    }

    private static long benchmarkWithoutCache() throws IOException {
        System.out.println("🐢 WITHOUT Buffer Pool (Every read hits disk)");
        System.out.println("─".repeat(70));

        if (DiskManager.databaseExists(BENCH_NO_CACHE)) {
            DiskManager.deleteDatabase(BENCH_NO_CACHE);
        }

        DiskBPlusTree<Integer, String> tree = new DiskBPlusTree<>(
                BENCH_NO_CACHE, TREE_ORDER, false);

        // Phase 1: Insert (write)
        long insertStart = System.nanoTime();
        for (int i = 0; i < INSERT_OPS; i++) {
            tree.insert(i, "V" + i);
        }
        long insertTime = System.nanoTime() - insertStart;

        System.out.printf("✓ Insert %,d records: %d ms%n",
                INSERT_OPS, insertTime / 1_000_000);

        // Phase 2: Search (read 10x more!)
        Random random = new Random(42);
        long searchStart = System.nanoTime();

        for (int i = 0; i < SEARCH_OPS; i++) {
            int key = random.nextInt(INSERT_OPS);
            String result = tree.search(key);
            if (result == null) {
                System.err.println("ERROR: Key " + key + " not found!");
            }
        }
        long searchTime = System.nanoTime() - searchStart;

        System.out.printf("✓ Search %,d ops: %d ms (%.1f ops/ms)%n",
                SEARCH_OPS, searchTime / 1_000_000,
                SEARCH_OPS / (searchTime / 1_000_000.0));

        long totalTime = insertTime + searchTime;
        System.out.printf("⏱️  TOTAL: %d ms%n", totalTime / 1_000_000);

        System.out.println("\n📊 I/O Statistics:");
        System.out.println(tree.getStatistics());

        tree.close();
        DiskManager.deleteDatabase(BENCH_NO_CACHE);

        return totalTime;
    }

    private static long benchmarkWithCache() throws IOException {
        System.out.println("🚀 WITH Buffer Pool (Hot pages cached in RAM)");
        System.out.println("─".repeat(70));

        if (DiskManager.databaseExists(BENCH_WITH_CACHE)) {
            DiskManager.deleteDatabase(BENCH_WITH_CACHE);
        }

        DiskBPlusTree<Integer, String> tree = new DiskBPlusTree<>(
                BENCH_WITH_CACHE, TREE_ORDER, true);

        // Phase 1: Insert
        long insertStart = System.nanoTime();
        for (int i = 0; i < INSERT_OPS; i++) {
            tree.insert(i, "V" + i);
        }
        long insertTime = System.nanoTime() - insertStart;

        System.out.printf("✓ Insert %,d records: %d ms%n",
                INSERT_OPS, insertTime / 1_000_000);

        // Phase 2: Search (same keys - should hit cache!)
        Random random = new Random(42);
        long searchStart = System.nanoTime();

        for (int i = 0; i < SEARCH_OPS; i++) {
            int key = random.nextInt(INSERT_OPS);
            String result = tree.search(key);
            if (result == null) {
                System.err.println("ERROR: Key " + key + " not found!");
            }
        }
        long searchTime = System.nanoTime() - searchStart;

        System.out.printf("✓ Search %,d ops: %d ms (%.1f ops/ms)%n",
                SEARCH_OPS, searchTime / 1_000_000,
                SEARCH_OPS / (searchTime / 1_000_000.0));

        long totalTime = insertTime + searchTime;
        System.out.printf("⏱️  TOTAL: %d ms%n", totalTime / 1_000_000);

        System.out.println("\n📊 Cache Statistics:");
        System.out.println(tree.getStatistics());

        tree.close();
        DiskManager.deleteDatabase(BENCH_WITH_CACHE);

        return totalTime;
    }
}
