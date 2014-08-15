package com.couchbase.lite.replicator2;

import com.couchbase.lite.Database;
import com.couchbase.lite.auth.Authenticator;
import com.couchbase.lite.internal.InterfaceAudience;
import com.couchbase.lite.support.HttpClientFactory;
import com.couchbase.lite.util.Log;
import com.couchbase.lite.util.Utils;
import com.github.oxo42.stateless4j.StateMachine;
import com.github.oxo42.stateless4j.delegates.Action1;
import com.github.oxo42.stateless4j.transitions.Transition;

import java.net.URL;
import java.util.List;
import java.util.Map;
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
    protected String lastSequence;
    protected Authenticator authenticator;
    protected String filterName;
    protected Map<String, Object> filterParams;
    protected List<String> documentIDs;
    protected Map<String, Object> requestHeaders;
    private String serverType;


    // the code assumes this is a _single threaded_ work executor.
    // if it's not, the behavior will be buggy.  I don't see a way to assert this in the code.
    protected ScheduledExecutorService workExecutor;

    protected StateMachine<ReplicationState, ReplicationTrigger> stateMachine;
    protected List<ChangeListener> changeListeners;
    protected Replication.Lifecycle lifecycle;
    protected ChangeListenerNotifyStyle changeListenerNotifyStyle;

    /**
     * Constructor
     */
    ReplicationInternal(Database db, URL remote, HttpClientFactory clientFactory, ScheduledExecutorService workExecutor, Replication.Lifecycle lifecycle, Replication parentReplication) {

        Utils.assertNotNull(lifecycle, "Must pass in a non-null lifecycle");

        this.parentReplication = parentReplication;
        this.db = db;
        this.remote = remote;
        this.clientFactory = clientFactory;
        this.workExecutor = workExecutor;
        this.lifecycle = lifecycle;

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
     * Actual work of starting the replication process.
     */
    abstract protected void beginReplicating();

    /**
     * Actual work of stopping the replication process.
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

    public Authenticator getAuthenticator() {
        return authenticator;
    }

    public void setAuthenticator(Authenticator authenticator) {
        this.authenticator = authenticator;
    }

    @InterfaceAudience.Private
    /* package */ boolean serverIsSyncGatewayVersion(String minVersion) {
        String prefix = "Couchbase Sync Gateway/";
        if (serverType == null) {
            return false;
        } else {
            if (serverType.startsWith(prefix)) {
                String versionString = serverType.substring(prefix.length());
                return versionString.compareTo(minVersion) >= 0;
            }

        }
        return false;
    }

    @InterfaceAudience.Private
    /* package */ void setServerType(String serverType) {
        this.serverType = serverType;
    }

    public Replication.Lifecycle getLifecycle() {
        return lifecycle;
    }

    public void setLifecycle(Replication.Lifecycle lifecycle) {
        this.lifecycle = lifecycle;
    }
}


