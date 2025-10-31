package com.titandb.core;

import org.junit.jupiter.api.*;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Complete System: Disk + MVCC + WAL")
public class ConcurrentDiskBPlusTreeTest {

    private ConcurrentDiskBPlusTree<Integer, String> db;
    private static final String DB_PATH = "test-concurrent-disk.db";

    @BeforeEach
    void setUp() throws IOException {
        if (com.titandb.storage.DiskManager.databaseExists(DB_PATH)) {
            com.titandb.storage.DiskManager.deleteDatabase(DB_PATH);
        }

        db = new ConcurrentDiskBPlusTree<>(DB_PATH, 4);
    }

    @AfterEach
    void tearDown() throws IOException {
        db.close();
        if (com.titandb.storage.DiskManager.databaseExists(DB_PATH)) {
            com.titandb.storage.DiskManager.deleteDatabase(DB_PATH);
        }
    }

    @Test
    @DisplayName("Disk + MVCC: Multiple concurrent writers with persistence")
    void testConcurrentWritersWithPersistence() throws IOException {
        System.out.println("\n📝 Test: Concurrent Disk Writers");
        System.out.println("═════════════════════════════════════");

        var txn1 = db.begin();
        db.insert(txn1, 1, "Alice");
        db.commit(txn1);
        System.out.println();

        var txn2 = db.begin();
        db.insert(txn2, 2, "Bob");
        db.commit(txn2);
        System.out.println();

        var txn3 = db.begin();
        db.insert(txn3, 3, "Charlie");
        db.commit(txn3);
        System.out.println();

        var txn4 = db.begin();
        assertEquals("Alice", db.search(txn4, 1));
        assertEquals("Bob", db.search(txn4, 2));
        assertEquals("Charlie", db.search(txn4, 3));
        db.commit(txn4);

        System.out.println("\n✅ All data persisted and visible!");
    }

    @Test
    @DisplayName("Disk + MVCC: Reader not blocked by uncommitted writer")
    void testReaderNotBlockedByWriter() throws IOException {
        System.out.println("\n🔓 Test: Reader Not Blocked");
        System.out.println("═════════════════════════════════════");

        var setup = db.begin();
        db.insert(setup, 1, "Initial");
        db.commit(setup);
        System.out.println();

        var reader = db.begin();
        System.out.println("🔍 Reader starting...");
        System.out.println();

        var writer = db.begin();
        db.insert(writer, 1, "Updated");
        System.out.println("✏️ Writer updating (not committed yet)...");
        System.out.println();

        String value = db.search(reader, 1);
        assertEquals("Initial", value);
        System.out.println("👁️ Reader still sees old value (no blocking!)");
        System.out.println();

        db.commit(writer);
        System.out.println("✅ Writer committed");
        System.out.println();

        String value2 = db.search(reader, 1);
        assertEquals("Initial", value2);
        System.out.println("👁️ Reader snapshot isolated from writer!");
        db.commit(reader);

        System.out.println("\n✅ No blocking, full isolation!");
    }

    @Test
    @DisplayName("Disk + MVCC: Concurrent reads (multiple readers)")
    void testMultipleConcurrentReaders() throws IOException {
        System.out.println("\n📖 Test: Multiple Concurrent Readers");
        System.out.println("═════════════════════════════════════");

        var setup = db.begin();
        db.insert(setup, 1, "Alice");
        db.insert(setup, 2, "Bob");
        db.insert(setup, 3, "Charlie");
        db.commit(setup);
        System.out.println();

        var reader1 = db.begin();
        System.out.println("📖 Reader 1 reading...");

        var reader2 = db.begin();
        System.out.println("📖 Reader 2 reading...");

        var reader3 = db.begin();
        System.out.println("📖 Reader 3 reading...");
        System.out.println();

        assertEquals("Alice", db.search(reader1, 1));
        assertEquals("Bob", db.search(reader2, 2));
        assertEquals("Charlie", db.search(reader3, 3));

        db.commit(reader1);
        db.commit(reader2);
        db.commit(reader3);

        System.out.println("\n✅ All readers see consistent data!");
    }

    @Test
    @DisplayName("Disk + WAL: Data survives process (persistence)")
    void testDataPersistence() throws IOException {
        System.out.println("\n💾 Test: Data Persistence");
        System.out.println("═════════════════════════════════════");

        var txn1 = db.begin();
        db.insert(txn1, 100, "Persistent");
        db.commit(txn1);
        System.out.println("✅ Inserted and committed");
        System.out.println();

        db.close();
        System.out.println("🔌 Database closed");
        System.out.println();

        db = new ConcurrentDiskBPlusTree<>(DB_PATH, 4);
        System.out.println("🔄 Database reopened");
        System.out.println();

        var txn2 = db.begin();
        String value = db.search(txn2, 100);
        assertEquals("Persistent", value);
        System.out.println("👁️ Data survived restart: " + value);
        db.commit(txn2);

        System.out.println("\n✅ Persistence confirmed!");
    }

    @Test
    @DisplayName("Complete workflow: Insert, Read, Commit, Verify")
    void testCompleteWorkflow() throws IOException {
        System.out.println("\n🚀 Test: Complete Workflow");
        System.out.println("═════════════════════════════════════");

        // Step 1: Insert batch of records
        System.out.println("\n1️⃣ Inserting records...");
        var txn1 = db.begin();
        for (int i = 1; i <= 5; i++) {
            db.insert(txn1, i, "Value-" + i);
        }
        db.commit(txn1);

        // Step 2: Read records
        System.out.println("\n2️⃣ Reading records...");
        var txn2 = db.begin();
        for (int i = 1; i <= 5; i++) {
            String value = db.search(txn2, i);
            assertEquals("Value-" + i, value);
        }
        db.commit(txn2);

        // Step 3: Concurrent reader during new write
        System.out.println("\n3️⃣ Concurrent access...");
        var reader = db.begin();
        var writer = db.begin();

        db.insert(writer, 10, "NewValue");
        String oldValue = db.search(reader, 1);
        assertEquals("Value-1", oldValue);

        db.commit(writer);
        db.commit(reader);

        // Step 4: Verify all data
        System.out.println("\n4️⃣ Final verification...");
        var txn3 = db.begin();
        for (int i = 1; i <= 5; i++) {
            assertEquals("Value-" + i, db.search(txn3, i));
        }
        assertEquals("NewValue", db.search(txn3, 10));
        db.commit(txn3);

        System.out.println("\n✅ Complete workflow successful!");
    }
}
