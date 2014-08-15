package com.couchbase.lite.replicator2;

import com.couchbase.lite.Database;
import com.couchbase.lite.support.HttpClientFactory;

import java.net.URL;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Pull Replication
 */
public class Puller extends Replication {

    public Puller(Database db, URL remote) {
        super(db, remote);
    }

    public Puller(Database db, URL remote, HttpClientFactory clientFactory, ScheduledExecutorService workExecutor) {
        super(db, remote, clientFactory, workExecutor);
    }

}
