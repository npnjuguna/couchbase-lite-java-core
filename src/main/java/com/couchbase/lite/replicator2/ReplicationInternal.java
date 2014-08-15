package com.couchbase.lite.replicator2;

import com.couchbase.lite.Database;
import com.couchbase.lite.internal.InterfaceAudience;
import com.couchbase.lite.support.HttpClientFactory;
import com.couchbase.lite.util.Log;
import com.github.oxo42.stateless4j.StateMachine;
import com.github.oxo42.stateless4j.delegates.Action1;
import com.github.oxo42.stateless4j.transitions.Transition;

import java.net.URL;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Internal Replication object that does the heavy lifting
 */
abstract class ReplicationInternal {

    // Change listeners can be called back synchronously or asynchronously.
    protected enum ChangeListenerNotifyStyle { SYNC, ASYNC };

    protected Replication parentReplication;
    protected Database db;
    protected URL remote;
    protected HttpClientFactory clientFactory;

    // the code assumes this is a _single threaded_ work executor.
    // if it's not, the behavior will be buggy.  I don't see a way to assert this in the code.
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
        workExecutor.submit(new Runnable() {
            @Override
            public void run() {
                stateMachine.fire(ReplicationTrigger.START);
            }
        });
    }

    /**
     * Trigger this replication to stop (async)
     */
    public void triggerStop() {
        workExecutor.submit(new Runnable() {
            @Override
            public void run() {
                stateMachine.fire(ReplicationTrigger.STOP_GRACEFUL);
            }
        });
    }

    /**
     * Actual work of starting the replication process.  OK to block here,
     * since it will only block the work executor, which may have multiple worker
     * threads.
     */
    protected void beginReplicating() {

        Log.d(Log.TAG_SYNC, "startReplicating()");

        if (!db.isOpen()) {

            String msg = String.format("Db: %s is not open, abort replication", db);
            parentReplication.setLastError(new Exception(msg));

            stateMachine.fire(ReplicationTrigger.STOP_IMMEDIATE);

        }

        // startChangeTracker();

        // start replicator ..


    }

    /**
     * Actual work of stopping the replication process.  OK to block here,
     * since it will only block the work executor.
     */
    protected void stopGraceful() {

        Log.d(Log.TAG_SYNC, "stopGraceful()");

        // stop things and possibly wait for them to stop ..

        try {
            Log.d(Log.TAG_SYNC, "sleeping ..");
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        stateMachine.fire(ReplicationTrigger.STOP_IMMEDIATE);

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

        stateMachine.configure(ReplicationState.RUNNING).onEntry(new Action1<Transition<ReplicationState, ReplicationTrigger>>() {
            @Override
            public void doIt(Transition<ReplicationState, ReplicationTrigger> transition) {
                notifyChangeListenersStateTransition(transition);
                ReplicationInternal.this.beginReplicating();
            }
        });
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
        stateMachine.configure(ReplicationState.STOPPING).onEntry(new Action1<Transition<ReplicationState, ReplicationTrigger>>() {
            @Override
            public void doIt(Transition<ReplicationState, ReplicationTrigger> transition) {
                notifyChangeListenersStateTransition(transition);
                ReplicationInternal.this.stopGraceful();
            }
        });
        stateMachine.configure(ReplicationState.STOPPED).onEntry(new Action1<Transition<ReplicationState, ReplicationTrigger>>() {
            @Override
            public void doIt(Transition<ReplicationState, ReplicationTrigger> transition) {
                notifyChangeListenersStateTransition(transition);
            }
        });

    }

    private void notifyChangeListenersStateTransition(Transition<ReplicationState, ReplicationTrigger> transition) {
        Replication.ChangeEvent changeEvent = new Replication.ChangeEvent(parentReplication);
        ReplicationStateTransition replicationStateTransition = new ReplicationStateTransition(transition);
        changeEvent.setTransition(replicationStateTransition);
        notifyChangeListeners(changeEvent);
    }




    /**
     * A delegate that can be used to listen for Replication changes.
     */
    @InterfaceAudience.Public
    public static interface ChangeListener {
        public void changed(Replication.ChangeEvent event);
    }

}


