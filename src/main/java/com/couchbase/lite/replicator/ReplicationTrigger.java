package com.couchbase.lite.replicator;


/**
 * The various triggers that a Replication state machine responds to
 */
enum ReplicationTrigger {
    START,
    WAITING_FOR_CHANGES,
    RETRY_FAILED_REVS,
    GO_OFFLINE,
    GO_ONLINE,
    STOP_GRACEFUL,
    STOP_IMMEDIATE
}

