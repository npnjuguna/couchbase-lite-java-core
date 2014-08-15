package com.couchbase.lite.replicator2;

import com.couchbase.lite.Database;
import com.couchbase.lite.support.HttpClientFactory;

import java.net.URL;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Pull Replication
 */
public class PullerInternal extends ReplicationInternal {


    public PullerInternal(Database db, URL remote, HttpClientFactory clientFactory, ScheduledExecutorService workExecutor, boolean isContinous, Replication parentReplication) {
        super(db, remote, clientFactory, workExecutor, isContinous, parentReplication);
    }

}
