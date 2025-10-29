package com.titandb.storage;

import com.titandb.core.LeafNode;
import com.titandb.core.InternalNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Serializes and deserializes B+ tree nodes to/from byte arrays.
 *
 * This enables nodes to be stored on disk and loaded back into memory.
 * We support both Integer keys and String values for now.
 *
 * @author Ujjwal
 */
public class NodeSerializer {
    private static final Logger logger = LoggerFactory.getLogger(NodeSerializer.class);

    // Node type markers
    private static final byte NODE_TYPE_LEAF = 0x01;
    private static final byte NODE_TYPE_INTERNAL = 0x02;

    /**
     * Serialize a leaf node to bytes.
     */
    public static byte[] serializeLeafNode(LeafNode<Integer, String> node) {
        // Calculate total size needed
        int size = 1 + 4 + 4; // Type + KeyCount + NextPageId

        for (int i = 0; i < node.getKeyCount(); i++) {
            size += 4;  // Key size
            size += 4;  // Key data (Integer)
            size += 4;  // Value size

            String value = node.getValue(i);
            if (value != null) {
                size += value.getBytes(StandardCharsets.UTF_8).length;
            }
        }

        // Allocate buffer
        ByteBuffer buffer = ByteBuffer.allocate(size);

        // Write header
        buffer.put(NODE_TYPE_LEAF);
        buffer.putInt(node.getKeyCount());
        buffer.putInt(-1);  // Next page ID placeholder

        // Write each entry
        for (int i = 0; i < node.getKeyCount(); i++) {
            Integer key = node.getKey(i);
            String value = node.getValue(i);

            // Write key
            buffer.putInt(4);  // Integer size
            buffer.putInt(key);

            // Write value
            if (value != null) {
                byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);
                buffer.putInt(valueBytes.length);
                buffer.put(valueBytes);
            } else {
                buffer.putInt(0);  // Null value
            }
        }

        logger.debug("Serialized leaf node: {} keys, {} bytes",
                node.getKeyCount(), size);

        return buffer.array();
    }

    /**
     * Deserialize bytes back to a leaf node.
     */
    public static LeafNode<Integer, String> deserializeLeafNode(byte[] data, int order) {
        ByteBuffer buffer = ByteBuffer.wrap(data);

        // Read header
        byte nodeType = buffer.get();
        if (nodeType != NODE_TYPE_LEAF) {
            throw new IllegalArgumentException("Invalid node type: " + nodeType);
        }

        int keyCount = buffer.getInt();
        int nextPageId = buffer.getInt();

        // Create leaf node
        LeafNode<Integer, String> node = new LeafNode<>(order);

        // Read each entry
        for (int i = 0; i < keyCount; i++) {
            // Read key
            int keySize = buffer.getInt();
            Integer key = buffer.getInt();

            // Read value
            int valueSize = buffer.getInt();
            String value = null;
            if (valueSize > 0) {
                byte[] valueBytes = new byte[valueSize];
                buffer.get(valueBytes);
                value = new String(valueBytes, StandardCharsets.UTF_8);
            }

            // Insert into node
            node.insert(key, value);
        }

        logger.debug("Deserialized leaf node: {} keys", keyCount);

        return node;
    }

    /**
     * Serialize an internal node to bytes.
     */
    public static byte[] serializeInternalNode(InternalNode<Integer, String> node) {
        // Calculate size
        int size = 1 + 4;  // Type + KeyCount
        size += node.getKeyCount() * (4 + 4);  // Keys (size + data)
        size += (node.getKeyCount() + 1) * 4;  // Child page IDs

        ByteBuffer buffer = ByteBuffer.allocate(size);

        // Write header
        buffer.put(NODE_TYPE_INTERNAL);
        buffer.putInt(node.getKeyCount());

        // Write keys using getKey() method
        for (int i = 0; i < node.getKeyCount(); i++) {
            buffer.putInt(4);  // Integer size
            buffer.putInt(node.getKey(i));  // ✅ Use getKey() method
        }

        // Write child page IDs (placeholder -1 for now)
        for (int i = 0; i <= node.getKeyCount(); i++) {
            buffer.putInt(-1);  // Placeholder
        }

        logger.debug("Serialized internal node: {} keys, {} bytes",
                node.getKeyCount(), size);

        return buffer.array();
    }

    /**
     * Deserialize bytes back to an internal node.
     */
    public static InternalNode<Integer, String> deserializeInternalNode(byte[] data, int order) {
        ByteBuffer buffer = ByteBuffer.wrap(data);

        // Read header
        byte nodeType = buffer.get();
        if (nodeType != NODE_TYPE_INTERNAL) {
            throw new IllegalArgumentException("Invalid node type: " + nodeType);
        }

        int keyCount = buffer.getInt();

        // Create internal node
        InternalNode<Integer, String> node = new InternalNode<>(order);

        // Read keys using addKey() method - FIXED!
        for (int i = 0; i < keyCount; i++) {
            int keySize = buffer.getInt();
            Integer key = buffer.getInt();
            node.addKey(key);  // ✅ Use addKey() instead of node.keys.add()
        }

        // Skip child page IDs for now
        for (int i = 0; i <= keyCount; i++) {
            buffer.getInt();  // Skip placeholder
        }

        logger.debug("Deserialized internal node: {} keys", keyCount);

        return node;
    }

    /**
     * Determine if serialized data is a leaf or internal node.
     */
    public static boolean isLeafNode(byte[] data) {
        return data.length > 0 && data[0] == NODE_TYPE_LEAF;
    }

    /**
     * Get the size of serialized data.
     */
    public static int getSerializedSize(byte[] data) {
        return data.length;
    }
}
