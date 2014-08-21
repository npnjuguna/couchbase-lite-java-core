package com.couchbase.lite.replicator2;

import com.couchbase.lite.Database;
import com.couchbase.lite.Misc;
import com.couchbase.lite.RevisionList;
import com.couchbase.lite.auth.Authenticator;
import com.couchbase.lite.auth.AuthenticatorImpl;
import com.couchbase.lite.internal.InterfaceAudience;
import com.couchbase.lite.internal.RevisionInternal;
import com.couchbase.lite.support.BatchProcessor;
import com.couchbase.lite.support.Batcher;
import com.couchbase.lite.support.HttpClientFactory;
import com.couchbase.lite.support.RemoteRequest;
import com.couchbase.lite.support.RemoteRequestCompletionBlock;
import com.couchbase.lite.util.Log;
import com.couchbase.lite.util.Utils;
import com.github.oxo42.stateless4j.StateMachine;
import com.github.oxo42.stateless4j.delegates.Action1;
import com.github.oxo42.stateless4j.transitions.Transition;

import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpResponseException;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
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
    protected Batcher<RevisionInternal> batcher;
    protected static final int PROCESSOR_DELAY = 500;
    protected static final int INBOX_CAPACITY = 100;
    protected static final int EXECUTOR_THREAD_POOL_SIZE = 5;
    protected ExecutorService remoteRequestExecutor;
    protected int asyncTaskCount;
    protected Throwable error;
    protected boolean lastSequenceChanged;
    private String remoteCheckpointDocID;
    protected Map<String, Object> remoteCheckpoint;


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
                try {
                    stateMachine.fire(ReplicationTrigger.START);
                } catch (Exception e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
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
                try {
                    stateMachine.fire(ReplicationTrigger.STOP_GRACEFUL);
                } catch (Exception e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
            }
        });
    }

    /**
     * Start the replication process.
     */
    protected void start() {

        if (!db.isOpen()) {

            String msg = String.format("Db: %s is not open, abort replication", db);
            parentReplication.setLastError(new Exception(msg));

            stateMachine.fire(ReplicationTrigger.STOP_IMMEDIATE);

            return;

        }

        // TODO:
        // db.addActiveReplication();

        // init batcher
        initBatcher();

        // init authorizer / authenticator
        initAuthorizer();

        // call goOnline (or trigger state change into online state)
        goOnline();


    }

    protected void initAuthorizer() {
        // TODO: add this back in  .. See Replication constructor

    }

    protected void initBatcher() {

        /*
         // TODO: add this back in  ..
        batcher = new Batcher<RevisionInternal>(workExecutor, INBOX_CAPACITY, PROCESSOR_DELAY, new BatchProcessor<RevisionInternal>() {
            @Override
            public void process(List<RevisionInternal> inbox) {

                try {
                    Log.v(Log.TAG_SYNC, "*** %s: BEGIN processInbox (%d sequences)", this, inbox.size());
                    processInbox(new RevisionList(inbox));
                    Log.v(Log.TAG_SYNC, "*** %s: END processInbox (lastSequence=%s)", this, lastSequence);
                    Log.v(Log.TAG_SYNC, "%s: batcher calling updateActive()", this);
                    updateActive();
                } catch (Exception e) {
                    Log.e(Log.TAG_SYNC,"ERROR: processInbox failed: ",e);
                    throw new RuntimeException(e);
                }
            }
        });
        */

    }

    protected void goOnline() {

        remoteRequestExecutor = Executors.newFixedThreadPool(EXECUTOR_THREAD_POOL_SIZE);
        checkSession();

    }

    @InterfaceAudience.Private
    protected void checkSession() {
        // REVIEW : This is not in line with the iOS implementation
        if (getAuthenticator() != null && ((AuthenticatorImpl)getAuthenticator()).usesCookieBasedLogin()) {
            checkSessionAtPath("/_session");
        } else {
            fetchRemoteCheckpointDoc();
        }
    }

    @InterfaceAudience.Private
    protected void checkSessionAtPath(final String sessionPath) {

        Log.v(Log.TAG_SYNC_ASYNC_TASK, "%s | %s: checkSessionAtPath() calling asyncTaskStarted()", this, Thread.currentThread());

        asyncTaskStarted();
        sendAsyncRequest("GET", sessionPath, null, new RemoteRequestCompletionBlock() {

            @Override
            public void onCompletion(Object result, Throwable error) {

                try {
                    if (error != null) {
                        // If not at /db/_session, try CouchDB location /_session
                        if (error instanceof HttpResponseException &&
                                ((HttpResponseException) error).getStatusCode() == 404 &&
                                sessionPath.equalsIgnoreCase("/_session")) {

                            checkSessionAtPath("_session");
                            return;
                        }
                        Log.e(Log.TAG_SYNC, this + ": Session check failed", error);
                        setError(error);

                    } else {
                        Map<String, Object> response = (Map<String, Object>) result;
                        Map<String, Object> userCtx = (Map<String, Object>) response.get("userCtx");
                        String username = (String) userCtx.get("name");
                        if (username != null && username.length() > 0) {
                            Log.d(Log.TAG_SYNC, "%s Active session, logged in as %s", this, username);
                            fetchRemoteCheckpointDoc();
                        } else {
                            Log.d(Log.TAG_SYNC, "%s No active session, going to login", this);
                            login();
                        }
                    }

                } finally {
                    Log.v(Log.TAG_SYNC_ASYNC_TASK, "%s | %s: checkSessionAtPath() calling asyncTaskFinished()", this, Thread.currentThread());

                    asyncTaskFinished(1);
                }
            }

        });
    }

    @InterfaceAudience.Private
    protected void login() {
        Map<String, String> loginParameters = ((AuthenticatorImpl)getAuthenticator()).loginParametersForSite(remote);
        if (loginParameters == null) {
            Log.d(Log.TAG_SYNC, "%s: %s has no login parameters, so skipping login", this, getAuthenticator());
            fetchRemoteCheckpointDoc();
            return;
        }

        final String loginPath = ((AuthenticatorImpl)getAuthenticator()).loginPathForSite(remote);

        Log.d(Log.TAG_SYNC, "%s: Doing login with %s at %s", this, getAuthenticator().getClass(), loginPath);

        Log.v(Log.TAG_SYNC_ASYNC_TASK, "%s | %s: login() calling asyncTaskStarted()", this, Thread.currentThread());

        asyncTaskStarted();
        sendAsyncRequest("POST", loginPath, loginParameters, new RemoteRequestCompletionBlock() {

            @Override
            public void onCompletion(Object result, Throwable e) {
                try {
                    if (e != null) {
                        Log.d(Log.TAG_SYNC, "%s: Login failed for path: %s", this, loginPath);
                        setError(e);
                    }
                    else {
                        Log.v(Log.TAG_SYNC, "%s: Successfully logged in!", this);
                        fetchRemoteCheckpointDoc();
                    }
                } finally {
                    Log.v(Log.TAG_SYNC_ASYNC_TASK, "%s | %s: login() calling asyncTaskFinished()", this, Thread.currentThread());

                    asyncTaskFinished(1);
                }
            }

        });

    }

    @InterfaceAudience.Private
    protected void setError(Throwable throwable) {
        // TODO
        /*
        if (error.code == NSURLErrorCancelled && $equal(error.domain, NSURLErrorDomain))
            return;
         */

        if (throwable != error) {
            Log.e(Log.TAG_SYNC, "%s: Progress: set error = %s", this, throwable);
            error = throwable;
            notifyChangeListeners();
        }

    }

    @InterfaceAudience.Private
    private void notifyChangeListeners() {
        // TODO: re-enable this: updateProgress();
        for (ChangeListener listener : changeListeners) {
            Replication.ChangeEvent changeEvent = new Replication.ChangeEvent(this.parentReplication);
            listener.changed(changeEvent);
        }

    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public synchronized void asyncTaskStarted() {
        Log.v(Log.TAG_SYNC_ASYNC_TASK, "%s: asyncTaskStarted %d -> %d", this, this.asyncTaskCount, this.asyncTaskCount + 1);
        if (asyncTaskCount++ == 0) {
            Log.v(Log.TAG_SYNC_ASYNC_TASK, "%s: asyncTaskStarted() calling updateActive()", this);
            // TODO: re-add this: updateActive();
        }
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public synchronized void asyncTaskFinished(int numTasks) {
        Log.v(Log.TAG_SYNC_ASYNC_TASK, "%s: asyncTaskFinished %d -> %d", this, this.asyncTaskCount, this.asyncTaskCount - numTasks);
        this.asyncTaskCount -= numTasks;
        assert(asyncTaskCount >= 0);
        if (asyncTaskCount == 0) {
            Log.v(Log.TAG_SYNC_ASYNC_TASK, "%s: asyncTaskFinished() calling updateActive()", this);
            // TODO: re-add this: updateActive();
        }
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public void sendAsyncRequest(String method, String relativePath, Object body, RemoteRequestCompletionBlock onCompletion) {
        try {
            String urlStr = buildRelativeURLString(relativePath);
            URL url = new URL(urlStr);
            sendAsyncRequest(method, url, body, onCompletion);
        } catch (MalformedURLException e) {
            Log.e(Log.TAG_SYNC, "Malformed URL for async request", e);
        }
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public Future sendAsyncRequest(String method, URL url, Object body, final RemoteRequestCompletionBlock onCompletion) {

        final RemoteRequest request = new RemoteRequest(workExecutor, clientFactory, method, url, body, getLocalDatabase(), getHeaders(), onCompletion);

        request.setAuthenticator(getAuthenticator());

        request.setOnPreCompletion(new RemoteRequestCompletionBlock() {
            @Override
            public void onCompletion(Object result, Throwable e) {
                if (serverType == null && result instanceof HttpResponse) {
                    HttpResponse response = (HttpResponse) result;
                    Header serverHeader = response.getFirstHeader("Server");
                    if (serverHeader != null) {
                        String serverVersion = serverHeader.getValue();
                        Log.v(Log.TAG_SYNC, "serverVersion: %s", serverVersion);
                        serverType = serverVersion;
                    }
                }
            }
        });


        if (remoteRequestExecutor.isTerminated()) {
            String msg = "sendAsyncRequest called, but remoteRequestExecutor has been terminated";
            throw new IllegalStateException(msg);
        }
        Future future = remoteRequestExecutor.submit(request);
        return future;

    }

    /**
     * Get the local database which is the source or target of this replication
     */
    @InterfaceAudience.Public
    public Database getLocalDatabase() {
        return db;
    }

    /**
     * Extra HTTP headers to send in all requests to the remote server.
     * Should map strings (header names) to strings.
     */
    @InterfaceAudience.Public
    public Map<String, Object> getHeaders() {
        return requestHeaders;
    }



    @InterfaceAudience.Private
    /* package */ String buildRelativeURLString(String relativePath) {

        // the following code is a band-aid for a system problem in the codebase
        // where it is appending "relative paths" that start with a slash, eg:
        //     http://dotcom/db/ + /relpart == http://dotcom/db/relpart
        // which is not compatible with the way the java url concatonation works.

        String remoteUrlString = remote.toExternalForm();
        if (remoteUrlString.endsWith("/") && relativePath.startsWith("/")) {
            remoteUrlString = remoteUrlString.substring(0, remoteUrlString.length() - 1);
        }
        return remoteUrlString + relativePath;
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public void fetchRemoteCheckpointDoc() {
        lastSequenceChanged = false;
        String checkpointId = remoteCheckpointDocID();
        final String localLastSequence = db.lastSequenceWithCheckpointId(checkpointId);

        Log.v(Log.TAG_SYNC_ASYNC_TASK, "%s | %s: fetchRemoteCheckpointDoc() calling asyncTaskStarted()", this, Thread.currentThread());

        asyncTaskStarted();
        sendAsyncRequest("GET", "/_local/" + checkpointId, null, new RemoteRequestCompletionBlock() {

            @Override
            public void onCompletion(Object result, Throwable e) {
                try {

                    if (e != null && !Utils.is404(e)) {
                        Log.w(Log.TAG_SYNC, "%s: error getting remote checkpoint", e, this);
                        setError(e);
                    } else {
                        if (e != null && Utils.is404(e)) {
                            Log.d(Log.TAG_SYNC, "%s: 404 error getting remote checkpoint %s, calling maybeCreateRemoteDB", this, remoteCheckpointDocID());
                            maybeCreateRemoteDB();
                        }
                        Map<String, Object> response = (Map<String, Object>) result;
                        remoteCheckpoint = response;
                        String remoteLastSequence = null;
                        if (response != null) {
                            remoteLastSequence = (String) response.get("lastSequence");
                        }
                        if (remoteLastSequence != null && remoteLastSequence.equals(localLastSequence)) {
                            lastSequence = localLastSequence;
                            Log.d(Log.TAG_SYNC, "%s: Replicating from lastSequence=%s", this, lastSequence);
                        } else {
                            Log.d(Log.TAG_SYNC, "%s: lastSequence mismatch: I had: %s, remote had: %s", this, localLastSequence, remoteLastSequence);
                        }
                        beginReplicating();
                    }
                } finally {
                    Log.v(Log.TAG_SYNC_ASYNC_TASK, "%s | %s: fetchRemoteCheckpointDoc() calling asyncTaskFinished()", this, Thread.currentThread());

                    asyncTaskFinished(1);
                }
            }

        });
    }

    @InterfaceAudience.Private
    /* package */ abstract void maybeCreateRemoteDB();

    /**
     * This is the _local document ID stored on the remote server to keep track of state.
     * Its ID is based on the local database ID (the private one, to make the result unguessable)
     * and the remote database's URL.
     *
     * @exclude
     */
    @InterfaceAudience.Private
    public String remoteCheckpointDocID() {

        if (remoteCheckpointDocID != null) {
            return remoteCheckpointDocID;
        } else {

            // TODO: Needs to be consistent with -hasSameSettingsAs: --
            // TODO: If a.remoteCheckpointID == b.remoteCheckpointID then [a hasSameSettingsAs: b]

            if (db == null) {
                return null;
            }

            // canonicalization: make sure it produces the same checkpoint id regardless of
            // ordering of filterparams / docids
            Map<String, Object> filterParamsCanonical = null;
            if (getFilterParams() != null) {
                filterParamsCanonical = new TreeMap<String, Object>(getFilterParams());
            }

            List<String> docIdsSorted = null;
            if (getDocIds() != null) {
                docIdsSorted = new ArrayList<String>(getDocIds());
                Collections.sort(docIdsSorted);
            }

            // use a treemap rather than a dictionary for purposes of canonicalization
            Map<String, Object> spec = new TreeMap<String, Object>();
            spec.put("localUUID", db.privateUUID());
            spec.put("remoteURL", remote.toExternalForm());
            spec.put("push", !isPull());
            spec.put("continuous", isContinuous());
            if (getFilter() != null) {
                spec.put("filter", getFilter());
            }
            if (filterParamsCanonical != null) {
                spec.put("filterParams", filterParamsCanonical);
            }
            if (docIdsSorted != null) {
                spec.put("docids", docIdsSorted);
            }

            byte[] inputBytes = null;
            try {
                inputBytes = db.getManager().getObjectMapper().writeValueAsBytes(spec);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            remoteCheckpointDocID = Misc.TDHexSHA1Digest(inputBytes);
            return remoteCheckpointDocID;

        }

    }

    /**
     * Name of an optional filter function to run on the source server. Only documents for
     * which the function returns true are replicated.
     *
     * For a pull replication, the name looks like "designdocname/filtername".
     * For a push replication, use the name under which you registered the filter with the Database.
     */
    @InterfaceAudience.Public
    public String getFilter() {
        return filterName;
    }

    /**
     * Is this a pull replication?  (Eg, it pulls data from Sync Gateway -> Device running CBL?)
     */
    @InterfaceAudience.Public
    public abstract boolean isPull();

    /**
     * Gets the documents to specify as part of the replication.
     */
    @InterfaceAudience.Public
    public List<String> getDocIds() {
        return documentIDs;
    }

    /**
     * Should the replication operate continuously, copying changes as soon as the
     * source database is modified? (Defaults to NO).
     */
    @InterfaceAudience.Public
    public boolean isContinuous() {
        return lifecycle == Replication.Lifecycle.CONTINUOUS;
    }

    /**
     * Parameters to pass to the filter function.  Should map strings to strings.
     */
    @InterfaceAudience.Public
    public Map<String, Object> getFilterParams() {
        return filterParams;
    }

    abstract protected void processInbox(RevisionList inbox);

    /**
     * After successfully authenticating and getting remote checkpoint,
     * begin the work of transferring documents.
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
                try {
                    changeListener.changed(changeEvent);
                } catch (Exception e) {
                    e.printStackTrace();
                    Log.e(Log.TAG_SYNC, "Exception notifying replication listener: %s", e);
                }
            }
        } else { // ASYNC
            workExecutor.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        for (ChangeListener changeListener : changeListeners) {
                            changeListener.changed(changeEvent);
                        }
                    } catch (Exception e) {
                        Log.e(Log.TAG_SYNC, "Exception notifying replication listener: %s", e);
                        throw new RuntimeException(e);
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
                ReplicationInternal.this.start();
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


