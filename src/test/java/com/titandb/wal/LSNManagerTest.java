package com.titandb.wal;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for LSNManager.
 */
@DisplayName("LSNManager Tests")
public class LSNManagerTest {

    @Test
    @DisplayName("LSN starts at 0 and increments")
    public void testLSNIncrement() {
        LSNManager manager = new LSNManager();

        assertEquals(0, manager.getCurrentLSN());

        assertEquals(1, manager.nextLSN());
        assertEquals(2, manager.nextLSN());
        assertEquals(3, manager.nextLSN());

        assertEquals(3, manager.getCurrentLSN());
    }

    @Test
    @DisplayName("LSN manager with custom start value")
    public void testCustomStartLSN() {
        LSNManager manager = new LSNManager(1000);

        assertEquals(1000, manager.getCurrentLSN());
        assertEquals(1001, manager.nextLSN());
        assertEquals(1002, manager.nextLSN());
    }

    @Test
    @DisplayName("Negative start LSN throws exception")
    public void testNegativeStartLSN() {
        assertThrows(IllegalArgumentException.class, () -> {
            new LSNManager(-1);
        });
    }

    @Test
    @DisplayName("Thread safety - concurrent LSN generation")
    public void testThreadSafety() throws InterruptedException {
        LSNManager manager = new LSNManager();
        int numThreads = 10;
        int operationsPerThread = 1000;

        Set<Long> generatedLSNs = ConcurrentHashMap.newKeySet();
        CountDownLatch latch = new CountDownLatch(numThreads);
        AtomicInteger errors = new AtomicInteger(0);

        // Spawn threads that generate LSNs concurrently
        for (int i = 0; i < numThreads; i++) {
            new Thread(() -> {
                try {
                    for (int j = 0; j < operationsPerThread; j++) {
                        long lsn = manager.nextLSN();
                        boolean wasNew = generatedLSNs.add(lsn);
                        if (!wasNew) {
                            errors.incrementAndGet(); // Duplicate LSN!
                        }
                    }
                } finally {
                    latch.countDown();
                }
            }).start();
        }

        latch.await(); // Wait for all threads

        // Verify
        assertEquals(0, errors.get(), "No duplicate LSNs should be generated");
        assertEquals(numThreads * operationsPerThread, generatedLSNs.size());
        assertEquals(numThreads * operationsPerThread, manager.getCurrentLSN());
    }

    @Test
    @DisplayName("Reset LSN value")
    public void testSetLSN() {
        LSNManager manager = new LSNManager();

        manager.nextLSN(); // 1
        manager.nextLSN(); // 2
        manager.nextLSN(); // 3

        manager.setLSN(1000);
        assertEquals(1000, manager.getCurrentLSN());
        assertEquals(1001, manager.nextLSN());
    }
}
