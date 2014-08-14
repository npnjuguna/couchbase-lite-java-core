package com.couchbase.lite.replicator2;

import com.couchbase.lite.Database;
import com.couchbase.lite.internal.InterfaceAudience;
import com.couchbase.lite.support.CouchbaseLiteHttpClientFactory;
import com.couchbase.lite.support.HttpClientFactory;

import java.net.URL;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * The external facade for the Replication API
 */
public class Replication implements ReplicationInternal.ChangeListener {

    protected Database db;
    protected URL remote;
    protected HttpClientFactory clientFactory;
    protected ScheduledExecutorService workExecutor;
    protected ReplicationInternal replicationInternal;
    protected boolean isContinous;
    protected List<ChangeListener> changeListeners;
    protected Throwable lastError;

    /**
     * Constructor
     */
    Replication(Database db, URL remote) {
        this(
                db,
                remote,
                new CouchbaseLiteHttpClientFactory(db.getPersistentCookieStore()),
                Executors.newSingleThreadScheduledExecutor()
        );

    }

    /**
     * Constructor
     */
    Replication(Database db, URL remote, HttpClientFactory clientFactory, ScheduledExecutorService workExecutor) {
        this.db = db;
        this.remote = remote;
        this.clientFactory = clientFactory;
        this.workExecutor = workExecutor;
        this.changeListeners = new CopyOnWriteArrayList<ChangeListener>();
    }

    /**
     * Starts the replication, asynchronously.
     */
    @InterfaceAudience.Public
    public void start() {
        replicationInternal = new ReplicationInternal(
                this.db,
                this.remote,
                this.clientFactory,
                this.workExecutor,
                this.isContinous,
                this
        );
        replicationInternal.addChangeListener(this);
        replicationInternal.triggerStart();
    }

    /**
     * True while the replication is running, False if it's stopped.
     * Note that a continuous replication never actually stops; it only goes idle waiting for new
     * data to appear.
     */
    @InterfaceAudience.Public
    public boolean isRunning() {
        if (replicationInternal == null) {
            return false;
        }
        return replicationInternal.stateMachine.isInState(ReplicationState.RUNNING);
    }

    /**
     * Stops the replication, asynchronously.
     */
    @InterfaceAudience.Public
    public void stop() {
        if (replicationInternal != null) {
            replicationInternal.triggerStop();
        }
    }

    /**
     * Is this replication continous?
     */
    @InterfaceAudience.Public
    public boolean isContinous() {
        return isContinous;
    }

    /**
     * Set whether this replication is continous
     */
    @InterfaceAudience.Public
    public void setContinous(boolean isContinous) {
        this.isContinous = isContinous;
    }

    /**
     * Adds a change delegate that will be called whenever the Replication changes.
     */
    @InterfaceAudience.Public
    public void addChangeListener(ChangeListener changeListener) {
        changeListeners.add(changeListener);
    }

    /**
     * This is called back for changes from the ReplicationInternal.
     * Simply propagate the events back to all listeners.
     */
    @Override
    public void changed(ChangeEvent event) {
        for (ChangeListener changeListener : changeListeners) {
            changeListener.changed(event);
        }
    }

    /**
     * The error status of the replication, or null if there have not been any errors since
     * it started.
     */
    @InterfaceAudience.Public
    public Throwable getLastError() {
        return lastError;
    }

    /**
     * Update the lastError
     */
    /* package */ void setLastError(Throwable lastError) {
        this.lastError = lastError;
    }

    /**
     * A delegate that can be used to listen for Replication changes.
     */
    @InterfaceAudience.Public
    public static interface ChangeListener {
        public void changed(ChangeEvent event);
    }

    /**
     * The type of event raised by a Replication when any of the following
     * properties change: mode, running, error, completed, total.
     */
    @InterfaceAudience.Public
    public static class ChangeEvent {

        private Replication source;

        public ChangeEvent(Replication source) {
            this.source = source;
        }

        public Replication getSource() {
            return source;
        }

    }


}
