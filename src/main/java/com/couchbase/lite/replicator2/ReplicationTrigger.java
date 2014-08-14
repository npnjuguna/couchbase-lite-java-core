package com.couchbase.lite.replicator2;


/**
 * The various triggers that a Replication state machine responds to
 */
enum ReplicationTrigger {
    START,
    STOP_GRACEFUL,
    STOP_IMMEDIATE
}

