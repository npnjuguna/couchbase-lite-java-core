package com.couchbase.lite.support;

import com.couchbase.lite.Database;
import com.couchbase.lite.auth.Authenticator;
import com.couchbase.lite.util.Log;
import com.couchbase.lite.util.Utils;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpUriRequest;

import java.net.URL;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Wraps a RemoteRequest with the ability to retry the request
 *
 * Huge caveat: this cannot work on a single threaded requestExecutor,
 * since it blocks while the "subrequests" are in progress, and during the sleeps
 * in between the retries.
 *
 */
public class RemoteRequestRetry implements Runnable {

    public static int MAX_RETRIES = 3;  // TODO: rename to MAX_ATTEMPTS
    public static int RETRY_DELAY_MS = 10 * 1000;

    protected ScheduledExecutorService workExecutor;
    protected ExecutorService requestExecutor;  // must have more than one thread

    protected final HttpClientFactory clientFactory;
    protected String method;
    protected URL url;
    protected Object body;
    protected Authenticator authenticator;
    protected RemoteRequestCompletionBlock onCompletionCaller;
    protected RemoteRequestCompletionBlock onPreCompletionCaller;

    private int retryCount;
    private Database db;
    protected HttpUriRequest request;

    private AtomicBoolean completedSuccessfully = new AtomicBoolean(false);
    private HttpResponse requestHttpResponse;
    private Object requestResult;
    private Throwable requestThrowable;

    protected Map<String, Object> requestHeaders;

    public RemoteRequestRetry(ExecutorService requestExecutor,
                              ScheduledExecutorService workExecutor,
                              HttpClientFactory clientFactory,
                              String method,
                              URL url,
                              Object body,
                              Database db,
                              Map<String, Object> requestHeaders,
                              RemoteRequestCompletionBlock onCompletionCaller) {

        this.requestExecutor = requestExecutor;
        this.clientFactory = clientFactory;
        this.method = method;
        this.url = url;
        this.body = body;
        this.onCompletionCaller = onCompletionCaller;
        this.workExecutor = workExecutor;
        this.requestHeaders = requestHeaders;
        this.db = db;

        Log.v(Log.TAG_SYNC, "%s: RemoteRequestRetry created, url: %s", this, url);

    }



    @Override
    public void run() {

        try {

            Log.d(Log.TAG_SYNC, "%s: RemoteRequestRetry run() started, url: %s", this, url);

            while (retryCount < MAX_RETRIES) {

                requestHttpResponse = null;
                requestResult = null;
                requestThrowable = null;

                RemoteRequest request = new RemoteRequest(
                        workExecutor,
                        clientFactory,
                        method,
                        url,
                        body,
                        db,
                        requestHeaders,
                        onCompletionInner
                );

                if (this.authenticator != null) {
                    request.setAuthenticator(this.authenticator);
                }
                if (this.onPreCompletionCaller != null) {
                    request.setOnPreCompletion(this.onPreCompletionCaller);
                }

                // wait for the future to return

                Future future = requestExecutor.submit(request);
                Log.d(Log.TAG_SYNC, "%s: RemoteRequestRetry waiting for future. url: %s", this, url);

                future.get();

                Log.d(Log.TAG_SYNC, "%s: RemoteRequestRetry future returned. url: %s", this, url);


                if (completedSuccessfully.get() == true) {
                    // we're done
                    Log.d(Log.TAG_SYNC, "%s: completedSuccessfully, we are done. url: %s", this, url);
                    onCompletionCaller.onCompletion(requestHttpResponse, requestResult, requestThrowable);
                    return;
                }

                retryCount += 1;

                if (retryCount < MAX_RETRIES) {
                    Log.d(Log.TAG_SYNC, "%s: Going to retry, sleeping. url: %s", this, url);

                    Thread.sleep(RETRY_DELAY_MS);

                    Log.d(Log.TAG_SYNC, "%s: Done sleeping. url: %s", this, url);
                }


            }

        } catch (Throwable e) {
            Log.e(Log.TAG_SYNC, "RemoteRequestRetry.run() exception: %s", e);
        }

        // exhausted attempts, callback to original caller with result.  requestThrowable
        // should contain most recent error that we received.
        onCompletionCaller.onCompletion(requestHttpResponse, requestResult, requestThrowable);

        Log.d(Log.TAG_SYNC, "%s: RemoteRequestRetry run() finished, url: %s", this, url);

    }

    RemoteRequestCompletionBlock onCompletionInner = new RemoteRequestCompletionBlock() {

        private void completed(HttpResponse httpResponse, Object result, Throwable e) {
            requestHttpResponse = httpResponse;
            requestResult = result;
            requestThrowable = e;
            completedSuccessfully.set(true);
        }

        @Override
        public void onCompletion(HttpResponse httpResponse, Object result, Throwable e) {
            Log.d(Log.TAG_SYNC, "%s: RemoteRequestRetry inner request finished, url: %s", this, url);

            if (e == null) {
                Log.d(Log.TAG_SYNC, "%s: RemoteRequestRetry was successful, calling callback url: %s", this, url);

                // just propagate completion block call back to the original caller
                completed(httpResponse, result, e);

            } else {

                if (Utils.isTransientError(httpResponse.getStatusLine())) {
                    if (retryCount == MAX_RETRIES) {
                        Log.d(Log.TAG_SYNC, "%s: RemoteRequestRetry failed, but transient error.  retrying. url: %s", this, url);
                        // ok, we're out of retries, propagate completion block call
                        completed(httpResponse, result, e);
                    } else {
                        // we're going to try again, so don't call the original caller's
                        // completion block yet.  Eventually it will get called though
                        Log.d(Log.TAG_SYNC, "%s: RemoteRequestRetry failed, but transient error.  NOT retrying. url: %s", this, url);

                    }

                } else {
                    Log.d(Log.TAG_SYNC, "%s: RemoteRequestRetry failed, non-transient error.  NOT retrying. url: %s", this, url);
                    // this isn't a transient error, so there's no point in retrying
                    completed(httpResponse, result, e);
                }

            }
        }
    };

    /**
     *  Set Authenticator for BASIC Authentication
     */
    public void setAuthenticator(Authenticator authenticator) {
        this.authenticator = authenticator;
    }

    public void setOnPreCompletionCaller(RemoteRequestCompletionBlock onPreCompletionCaller) {
        this.onPreCompletionCaller = onPreCompletionCaller;
    }
}
