package com.couchbase.lite.replicator2;

import com.couchbase.lite.Database;
import com.couchbase.lite.replicator.ChangeTracker;
import com.couchbase.lite.replicator.ChangeTrackerClient;
import com.couchbase.lite.support.HttpClientFactory;
import com.couchbase.lite.util.Log;

import org.apache.http.client.HttpClient;

import java.net.URL;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Pull Replication
 */
public class PullerInternal extends ReplicationInternal implements ChangeTrackerClient{

    private ChangeTracker changeTracker;

    public PullerInternal(Database db, URL remote, HttpClientFactory clientFactory, ScheduledExecutorService workExecutor, Replication.Lifecycle lifecycle, Replication parentReplication) {
        super(db, remote, clientFactory, workExecutor, lifecycle, parentReplication);
    }

    /**
     * Actual work of starting the replication process.
     */
    protected void beginReplicating() {

        Log.d(Log.TAG_SYNC, "startReplicating()");

        if (!db.isOpen()) {

            String msg = String.format("Db: %s is not open, abort replication", db);
            parentReplication.setLastError(new Exception(msg));

            stateMachine.fire(ReplicationTrigger.STOP_IMMEDIATE);

            return;

        }

        startChangeTracker();

        // start replicator ..

    }



    private void startChangeTracker() {

        ChangeTracker.ChangeTrackerMode changeTrackerMode;

        // it always starts out as OneShot, but if its a continuous replication
        // it will switch to longpoll later.
        changeTrackerMode = ChangeTracker.ChangeTrackerMode.OneShot;

        Log.w(Log.TAG_SYNC, "%s: starting ChangeTracker with since=%s mode=%s", this, lastSequence, changeTrackerMode);
        changeTracker = new ChangeTracker(remote, changeTrackerMode, true, lastSequence, this);
        changeTracker.setAuthenticator(getAuthenticator());
        Log.w(Log.TAG_SYNC, "%s: started ChangeTracker %s", this, changeTracker);

        if (filterName != null) {
            changeTracker.setFilterName(filterName);
            if (filterParams != null) {
                changeTracker.setFilterParams(filterParams);
            }
        }
        changeTracker.setDocIDs(documentIDs);
        changeTracker.setRequestHeaders(requestHeaders);
        changeTracker.setContinuous(lifecycle == Replication.Lifecycle.CONTINUOUS);

        Log.v(Log.TAG_SYNC_ASYNC_TASK, "%s | %s: beginReplicating() calling asyncTaskStarted()", this, Thread.currentThread());

        changeTracker.setUsePOST(serverIsSyncGatewayVersion("0.93"));
        changeTracker.start();

    }

    @Override
    public HttpClient getHttpClient() {

        HttpClient httpClient = this.clientFactory.getHttpClient();

        return httpClient;
    }

    @Override
    public void changeTrackerReceivedChange(final Map<String, Object> change) {
        workExecutor.submit(new Runnable() {
            @Override
            public void run() {
                Log.d(Log.TAG_SYNC, "changeTrackerReceivedChange: %s", change);
            }
        });
    }

    @Override
    public void changeTrackerStopped(ChangeTracker tracker) {

        workExecutor.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    Log.d(Log.TAG_SYNC, "changeTrackerStopped.  lifecycle: %s", lifecycle);
                    switch (lifecycle) {
                        case ONESHOT:
                            Log.d(Log.TAG_SYNC, "fire STOP_GRACEFUL");
                            stateMachine.fire(ReplicationTrigger.STOP_GRACEFUL);
                            break;
                        case CONTINUOUS:
                            String msg = String.format("Change tracker stopped during continuous replication");
                            parentReplication.setLastError(new Exception(msg));
                            stateMachine.fire(ReplicationTrigger.STOP_GRACEFUL);
                            break;
                        default:
                            throw new RuntimeException(String.format("Unknown lifecycle: %s", lifecycle));

                    }
                } catch (RuntimeException e) {
                    e.printStackTrace();
                    throw e;
                }
            }
        });

    }

    @Override
    public void changeTrackerFinished(ChangeTracker tracker) {
        workExecutor.submit(new Runnable() {
            @Override
            public void run() {
                Log.d(Log.TAG_SYNC, "changeTrackerFinished");
            }
        });
    }

    @Override
    public void changeTrackerCaughtUp() {
        workExecutor.submit(new Runnable() {
            @Override
            public void run() {
                Log.d(Log.TAG_SYNC, "changeTrackerCaughtUp");
            }
        });
    }
}
