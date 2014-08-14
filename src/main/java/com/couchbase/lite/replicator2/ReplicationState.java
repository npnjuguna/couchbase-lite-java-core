package com.couchbase.lite.replicator2;

/**
 * The various states that a Replication can be in
 */
enum ReplicationState {
    INITIAL,
    RUNNING,
    STOPPING,
    STOPPED
}
