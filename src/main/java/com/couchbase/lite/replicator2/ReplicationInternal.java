package com.couchbase.lite.replicator2;

import com.couchbase.lite.Database;
import com.couchbase.lite.internal.InterfaceAudience;
import com.couchbase.lite.support.HttpClientFactory;
import com.github.oxo42.stateless4j.StateMachine;
import com.github.oxo42.stateless4j.delegates.Action;

import java.net.URL;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Internal Replication object that does the heavy lifting
 */
class ReplicationInternal {

    // Change listeners can be called back synchronously or asynchronously.
    protected enum ChangeListenerNotifyStyle { SYNC, ASYNC };

    protected Replication parentReplication;
    protected Database db;
    protected URL remote;
    protected HttpClientFactory clientFactory;
    protected ScheduledExecutorService workExecutor;
    protected StateMachine<ReplicationState, ReplicationTrigger> stateMachine;
    protected List<ChangeListener> changeListeners;
    protected boolean isContinous;
    protected ChangeListenerNotifyStyle changeListenerNotifyStyle;

    /**
     * Constructor
     */
    ReplicationInternal(Database db, URL remote, HttpClientFactory clientFactory, ScheduledExecutorService workExecutor, boolean isContinous, Replication parentReplication) {

        this.parentReplication = parentReplication;
        this.db = db;
        this.remote = remote;
        this.clientFactory = clientFactory;
        this.workExecutor = workExecutor;
        this.isContinous = isContinous;

        changeListeners = new CopyOnWriteArrayList<ChangeListener>();

        changeListenerNotifyStyle = ChangeListenerNotifyStyle.SYNC;

        initializeStateMachine();

    }

    /**
     * Trigger this replication to start (async)
     */
    public void triggerStart() {
        stateMachine.fire(ReplicationTrigger.START);
    }

    /**
     * Trigger this replication to stop (async)
     */
    public void triggerStop() {
        stateMachine.fire(ReplicationTrigger.STOP_GRACEFUL);
    }

    /**
     * Actual work of starting the replication process.  OK to block here,
     * since it will only block the work executor, which may have multiple worker
     * threads.
     */
    protected void startReplicating() {

        // startChangeTracker();
        if (!db.isOpen()) {

            String msg = String.format("Db: %s is not open, abort replication", db);
            parentReplication.setLastError(new Exception(msg));

            stateMachine.fire(ReplicationTrigger.STOP_IMMEDIATE);

        }

    }

    /**
     * Notify all change listeners of a ChangeEvent
     */
    private void notifyChangeListeners(final Replication.ChangeEvent changeEvent) {
        if (changeListenerNotifyStyle == ChangeListenerNotifyStyle.SYNC) {
            for (ChangeListener changeListener : changeListeners) {
                changeListener.changed(changeEvent);
            }
        } else { // ASYNC
            workExecutor.submit(new Runnable() {
                @Override
                public void run() {
                    for (ChangeListener changeListener : changeListeners) {
                        changeListener.changed(changeEvent);
                    }
                }
            });
        }
    }

    /**
     * Adds a change delegate that will be called whenever the Replication changes.
     */
    @InterfaceAudience.Public
    public void addChangeListener(ChangeListener changeListener) {
        changeListeners.add(changeListener);
    }


    /**
     * Initialize the state machine which defines the overall behavior of the replication
     * object.
     */
    protected void initializeStateMachine() {

        stateMachine = new StateMachine<ReplicationState, ReplicationTrigger>(ReplicationState.INITIAL);
        stateMachine.configure(ReplicationState.INITIAL).permit(
                ReplicationTrigger.START,
                ReplicationState.RUNNING
        );

        stateMachine.configure(ReplicationState.RUNNING).ignore(ReplicationTrigger.START);
        stateMachine.configure(ReplicationState.STOPPING).ignore(ReplicationTrigger.STOP_GRACEFUL);
        stateMachine.configure(ReplicationState.STOPPED).ignore(ReplicationTrigger.STOP_GRACEFUL);
        stateMachine.configure(ReplicationState.STOPPED).ignore(ReplicationTrigger.STOP_IMMEDIATE);

        stateMachine.configure(ReplicationState.RUNNING).onEntry(new ActionReplicationStarted());
        stateMachine.configure(ReplicationState.RUNNING).permit(
                ReplicationTrigger.STOP_IMMEDIATE,
                ReplicationState.STOPPED
        );
        stateMachine.configure(ReplicationState.RUNNING).permit(
                ReplicationTrigger.STOP_GRACEFUL,
                ReplicationState.STOPPING
        );
        stateMachine.configure(ReplicationState.STOPPING).permit(
                ReplicationTrigger.STOP_IMMEDIATE,
                ReplicationState.STOPPED
        );
        stateMachine.configure(ReplicationState.STOPPING).onEntry(new Action() {
            @Override
            public void doIt() {
                // TODO: graceful shutdown of replicator
                stateMachine.fire(ReplicationTrigger.STOP_IMMEDIATE);
            }
        });
        stateMachine.configure(ReplicationState.STOPPED).onEntry(new Action() {
            @Override
            public void doIt() {
                Replication.ChangeEvent changeEvent = new Replication.ChangeEvent(parentReplication);
                notifyChangeListeners(changeEvent);
            }
        });

    }


    /**
     * When the replication is started, this action will be run.
     * It's an async wrapper for the startReplicating() method.
     */
    class ActionReplicationStarted implements Action {

        @Override
        public void doIt() {

            // This is run asynchronously on the work executor so that
            // the state machine remains live/reactive.
            ReplicationInternal.this.workExecutor.submit(new Runnable() {
                @Override
                public void run() {
                    ReplicationInternal.this.startReplicating();
                }
            });
        }
    }

    /**
     * A delegate that can be used to listen for Replication changes.
     */
    @InterfaceAudience.Public
    public static interface ChangeListener {
        public void changed(Replication.ChangeEvent event);
    }

}


