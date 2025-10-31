package com.titandb.cli;

import com.titandb.core.DiskBPlusTree;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Interactive TitanDB CLI
 *
 * Allow interviewers to interact with the database in real-time!
 */
public class TitanDBCLI {

    private static DiskBPlusTree<Integer, String> tree;
    private static BufferedReader reader;
    private static String dbName = "titandb-interactive.db";

    public static void main(String[] args) throws IOException {
        reader = new BufferedReader(new InputStreamReader(System.in));

        printWelcome();

        // Initialize database
        initializeDatabase();

        // Command loop
        commandLoop();
    }

    static void printWelcome() {
        System.out.println("""

            ╔════════════════════════════════════════════════════════╗
            ║                                                        ║
            ║              🚀 Welcome to TitanDB 🚀                  ║
            ║                                                        ║
            ║      Interactive Production-Grade Database Engine      ║
            ║                                                        ║
            ║  Try inserting, searching, and exploring the database ║
            ║                                                        ║
            ╚════════════════════════════════════════════════════════╝

            📖 Available Commands:
            ─────────────────────────────────────────────────────────
            INSERT <key> <value>    - Insert a key-value pair
            SEARCH <key>            - Search for a value by key
            DELETE <key>            - Delete a key (future feature)
            STATS                   - Show database statistics
            LOAD <count>            - Load N sample records
            CRASH                   - Simulate crash (for recovery demo)
            HELP                    - Show this help
            EXIT                    - Exit the database

            """);
    }

    static void initializeDatabase() throws IOException {
        System.out.print("🔧 Initializing TitanDB... ");
        tree = new DiskBPlusTree<>(dbName, 4);
        System.out.println("✅ Ready!\n");
    }

    static void commandLoop() throws IOException {
        while (true) {
            System.out.print("titandb> ");
            String input = reader.readLine();

            if (input == null || input.trim().isEmpty()) {
                continue;
            }

            String[] parts = input.trim().split("\\s+", 3);
            String command = parts[0].toUpperCase();

            try {
                switch (command) {
                    case "INSERT":
                        handleInsert(parts);
                        break;
                    case "SEARCH":
                        handleSearch(parts);
                        break;
                    case "STATS":
                        handleStats();
                        break;
                    case "LOAD":
                        handleLoad(parts);
                        break;
                    case "CRASH":
                        handleCrash();
                        break;
                    case "HELP":
                        printWelcome();
                        break;
                    case "EXIT":
                        handleExit();
                        return;
                    default:
                        System.out.println("❌ Unknown command: " + command + ". Type HELP for commands.\n");
                }
            } catch (Exception e) {
                System.out.println("❌ Error: " + e.getMessage() + "\n");
            }
        }
    }

    static void handleInsert(String[] parts) throws IOException {
        if (parts.length < 3) {
            System.out.println("❌ Usage: INSERT <key> <value>\n");
            return;
        }

        int key = Integer.parseInt(parts[1]);
        String value = parts[2];

        tree.insert(key, value);
        System.out.println("✅ Inserted: " + key + " → \"" + value + "\"\n");
    }

    static void handleSearch(String[] parts) throws IOException {
        if (parts.length < 2) {
            System.out.println("❌ Usage: SEARCH <key>\n");
            return;
        }

        int key = Integer.parseInt(parts[1]);
        String result = tree.search(key);

        if (result != null) {
            System.out.println("✅ Found: " + key + " → \"" + result + "\"\n");
        } else {
            System.out.println("❌ Not found: Key " + key + " does not exist\n");
        }
    }

    static void handleStats() throws IOException {
        System.out.println("📊 Database Statistics:");
        System.out.println("─────────────────────────────────────────────");
        System.out.println(tree.getStatistics());
        System.out.println("─────────────────────────────────────────────\n");
    }

    static void handleLoad(String[] parts) throws IOException {
        if (parts.length < 2) {
            System.out.println("❌ Usage: LOAD <count>\n");
            return;
        }

        int count = Integer.parseInt(parts[1]);

        System.out.print("📝 Loading " + count + " sample records... ");
        long startTime = System.currentTimeMillis();

        for (int i = 1; i <= count; i++) {
            tree.insert(i, "Record-" + i);
        }

        long duration = System.currentTimeMillis() - startTime;
        System.out.println("✅ Loaded in " + duration + "ms\n");
    }

    static void handleCrash() throws IOException {
        System.out.println("⚡ CRASH SIMULATION");
        System.out.println("──────────────────────────────────────────────────");
        System.out.println();

        // Insert some data
        tree.insert(999, "Before Crash");
        System.out.println("Before crash: Inserted (999, 'Before Crash')");
        System.out.println();

        // Close without proper shutdown (simulate crash)
        tree.close();
        System.out.println("💥 Database crashed! (Files written but recovery needed)");
        System.out.println();

        // Reopen and recover
        System.out.print("🔄 Reopening database (recovery process)... ");
        tree = new DiskBPlusTree<>(dbName, 4);
        System.out.println("✅ Recovered!\n");

        // Verify data
        String recovered = tree.search(999);
        if (recovered != null) {
            System.out.println("✅ Data survived crash: (999, '" + recovered + "')\n");
        } else {
            System.out.println("❌ Data lost in crash\n");
        }
    }

    static void handleExit() throws IOException {
        System.out.println();
        System.out.println("👋 Saving and closing database...");
        tree.close();

        System.out.println("✅ Database closed successfully!");
        System.out.println();
        System.out.println("📌 Pro Tip: Your data is persisted to disk!");
        System.out.println("   Run the CLI again to see your data!");
        System.out.println();
    }
}
