package com.titandb.concurrency;

/**
 * A versioned value in MVCC
 *
 * Each record can have multiple versions
 * Each version has a creator transaction ID
 */
public class VersionedValue<V> {

    static class Version<V> {
        public final long createdBy;
        public final long createdAt;
        public final V value;
        public final boolean deleted;

        Version(long createdBy, V value, boolean deleted) {
            this.createdBy = createdBy;
            this.createdAt = System.currentTimeMillis();
            this.value = value;
            this.deleted = deleted;
        }
    }

    private final String key;
    private final java.util.List<Version<V>> versions = new java.util.ArrayList<>();

    public VersionedValue(String key) {
        this.key = key;
    }

    /**
     * Create a new version
     */
    public void addVersion(long txnId, V value) {
        versions.add(new Version<>(txnId, value, false));
    }

    /**
     * Mark as deleted
     */
    public void deleteVersion(long txnId) {
        versions.add(new Version<>(txnId, null, true));
    }

    public java.util.List<Version<V>> getVersions() {
        return new java.util.ArrayList<>(versions);
    }

    @Override
    public String toString() {
        return "VersionedValue{" +
                "key='" + key + '\'' +
                ", versions=" + versions.size() +
                '}';
    }
}
