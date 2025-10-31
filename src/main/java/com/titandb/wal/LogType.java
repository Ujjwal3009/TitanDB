package com.titandb.wal;

import org.slf4j.event.LoggingEvent;

/**
        * Types of log records in the Write-Ahead Log (WAL).
        *
        * Each type represents a different operation that can be logged.
        * This is similar to SQL DDL/DML statement types.
         * Categories:
         * - Transaction control: BEGIN, COMMIT, ABORT
         * - Data modifications: INSERT, UPDATE, DELETE
         * - System operations: CHECKPOINT, CLR
 */
public enum LogType {

    /**
     * Transaction started.
     * Marks the beginning of a transaction.
     * Example: BEGIN TRANSACTION txn_id=77
     */
    BEGIN(1),

    /**
     * Transaction committed successfully.
     * All changes are now permanent (durable).
     * Example: COMMIT txn_id=77
     */

    COMMIT(2),

    /**
     * Transaction aborted/rolled back.
     * All changes must be undone.
     * Example: ABORT txn_id=77
     */
    ABORT(3),

    /**
     * Insert operation.
     * Adds a new key-value pair to a page.
     * Contains: pageId, key, new value
     * UNDO: Delete the key
     * REDO: Insert the key-value pair
     */
    INSERT(4),

    /**
     * Update operation.
     * Modifies an existing key's value.
     * Contains: pageId, key, old value, new value
     * UNDO: Restore old value
     * REDO: Apply new value
     */
    UPDATE(5),

    /**
     * Delete operation.
     * Removes a key-value pair.
     * Contains: pageId, key, old value
     * UNDO: Re-insert key-value pair
     * REDO: Delete the key
     */

    DELETE(6),
    /**
     * Checkpoint record.
     * Indicates all dirty pages have been flushed to disk.
     * Allows truncating older log entries.
     * Contains: dirty page table, transaction table
     */
    CHECKPOINT(7),

    /**
     * Compensation Log Record (CLR).
     * Created during UNDO operations to prevent re-undo.
     * If we crash while rolling back transaction X, CLR ensures
     * we don't undo the same operation twice.
     * Contains: undone LSN
     */
    CLR(8);

    private final int typeCode;

    LogType(int typeCode){
        this.typeCode = typeCode;
    }

    public  int getTypeCode(){
        return this.typeCode;
    }

    public  static  LogType fromTypeCode(int typeCode){
        for(LogType type : LogType.values()){
            if(type.typeCode == typeCode){
                 return  type;
            }
        }
        throw  new IllegalArgumentException("Invalid log type code: " + typeCode);
    }

}
