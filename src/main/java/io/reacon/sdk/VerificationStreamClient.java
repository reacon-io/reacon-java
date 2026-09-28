// Copyright Reacon contributors. Licensed under Apache-2.0.
package io.reacon.sdk;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.reacon.sdk.model.*;
import okhttp3.*;
import okhttp3.sse.*;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/** Explicit, lazy SSE requests. Never retries, reconnects, or follows redirects. */
public final class VerificationStreamClient implements AutoCloseable {
    private final String key;
    private final HttpUrl baseUrl;
    private final OkHttpClient http;
    private final boolean owned;
    private final ScheduledExecutorService timers;
    private final Set<VerificationStream> streams = ConcurrentHashMap.newKeySet();
    private volatile boolean closed;

    public VerificationStreamClient(String apiKey, String baseUrl) { this(apiKey, baseUrl, null); }
    public VerificationStreamClient(String apiKey) { this(apiKey, "https://api.reacon.io", null); }
    /** Injected transport's pool and dispatcher remain caller-owned. Request policies are overridden on a derived client. */
    public VerificationStreamClient(String apiKey, String baseUrl, OkHttpClient transport) {
        if (apiKey == null || apiKey.trim().isEmpty()) throw new IllegalArgumentException("apiKey is required");
        this.key = apiKey;
        this.baseUrl = HttpUrl.get(baseUrl.replaceAll("/+$", "") + "/");
        owned = transport == null;
        http = HttpPolicy.client(owned ? new OkHttpClient() : transport).newBuilder()
            .callTimeout(0, TimeUnit.MILLISECONDS).build();
        timers = Executors.newSingleThreadScheduledExecutor(r -> { Thread t = new Thread(r, "reacon-stream-timeouts"); t.setDaemon(true); return t; });
    }
    @Override public void close() {
        closed = true;
        for (VerificationStream stream : streams) stream.close();
        timers.shutdownNow();
        if (owned) { http.dispatcher().executorService().shutdown(); http.connectionPool().evictAll(); }
    }
    public static final class Options {
        public final Duration idleTimeout, totalTimeout;
        public final String onlyIfFree, cacheMaxAge;
        public Options() { this(null, null, Duration.ofSeconds(30), Duration.ofMinutes(5)); }
        public Options(String onlyIfFree, String cacheMaxAge, Duration idleTimeout, Duration totalTimeout) {
            if (idleTimeout == null || totalTimeout == null || idleTimeout.toMillis() <= 0 || totalTimeout.toMillis() <= 0)
                throw new IllegalArgumentException("Positive timeouts are required");
            if (onlyIfFree != null && !Arrays.asList("true", "false").contains(onlyIfFree)) throw new IllegalArgumentException("Invalid onlyIfFree");
            if (cacheMaxAge != null && !Arrays.asList("live", "1d", "1w", "1m").contains(cacheMaxAge)) throw new IllegalArgumentException("Invalid cacheMaxAge");
            this.onlyIfFree = onlyIfFree; this.cacheMaxAge = cacheMaxAge; this.idleTimeout = idleTimeout; this.totalTimeout = totalTimeout;
        }
    }
    public static class ProtocolException extends RuntimeException { public ProtocolException(String message) { super(message); } }
    public static class TransportException extends RuntimeException { public TransportException() { super("Reacon stream transport failure"); } }
    public static class StreamTimeoutException extends RuntimeException {
        public final String phase;
        public StreamTimeoutException(String phase) { super("Reacon " + phase + " timeout"); this.phase = phase; }
    }
    public static class StreamApiException extends RuntimeException {
        public final int status;
        public final Headers headers;
        public final Object body;
        public final VerificationStreamError event;
        StreamApiException(Response response, Object body, VerificationStreamError event) {
            super(event == null ? "Reacon returned HTTP " + response.code() : "Reacon stream failed: " + event.getCode());
            this.status = response.code(); this.headers = response.headers(); this.body = body; this.event = event;
        }
        public String requestId() { return headers.get("x-request-id"); }
    }
    public abstract static class Event {
        public final JsonObject raw;
        Event(JsonObject raw) { this.raw = raw; }
    }
    public static final class Stage extends Event { public final VerificationStage data; Stage(JsonObject raw) { super(raw); data = JSON.getGson().fromJson(raw, VerificationStage.class); } }
    public static final class Progress extends Event { public final VerificationProgress data; Progress(JsonObject raw) { super(raw); data = JSON.getGson().fromJson(raw, VerificationProgress.class); } }
    public static final class Final extends Event { public final VerificationFinal data; Final(JsonObject raw) { super(raw); data = JSON.getGson().fromJson(raw, VerificationFinal.class); } }
    public static final class Unknown extends Event { Unknown(JsonObject raw) { super(raw); } }

    /** Use try-with-resources, including when stopping iteration early. Merely creating a stream sends no request. */
    public VerificationStream streamVerification(String email, Options options) {
        if (closed) throw new IllegalStateException("Client is closed");
        if (email == null || email.isEmpty()) throw new IllegalArgumentException("email is required");
        return new VerificationStream(email, options == null ? new Options() : options);
    }
    public final class VerificationStream implements Iterator<Event>, Iterable<Event>, AutoCloseable {
        private final String email;
        private final Options options;
        private final ArrayBlockingQueue<Object> queue = new ArrayBlockingQueue<>(1);
        private final Object end = new Object();
        private final AtomicBoolean finished = new AtomicBoolean(), disposed = new AtomicBoolean();
        private EventSource source;
        private ScheduledFuture<?> timeout;
        private Object next;
        private volatile Object terminal;
        private boolean started;
        private Response response;
        VerificationStream(String email, Options options) { this.email = email; this.options = options; }
        @Override public Iterator<Event> iterator() { return this; }
        private synchronized void start() {
            if (started || disposed.get()) return;
            if (closed) throw new IllegalStateException("Client is closed");
            started = true; streams.add(this);
            HttpUrl.Builder url = baseUrl.newBuilder().addPathSegments("v1/verify").addQueryParameter("email", email);
            if (options.onlyIfFree != null) url.addQueryParameter("onlyIfFree", options.onlyIfFree);
            if (options.cacheMaxAge != null) url.addQueryParameter("cacheMaxAge", options.cacheMaxAge);
            Request request = new Request.Builder().url(url.build()).header("X-API-Key", key).header("Accept", "text/event-stream").build();
            timeout = timers.schedule(() -> complete(new StreamTimeoutException("total")), options.totalTimeout.toMillis(), TimeUnit.MILLISECONDS);
            source = EventSources.createFactory(http.newBuilder().readTimeout(options.idleTimeout.toMillis(), TimeUnit.MILLISECONDS).build())
                .newEventSource(request, new EventSourceListener() {
                    @Override public void onOpen(EventSource source, Response value) { response = value; }
                    @Override public void onEvent(EventSource source, String id, String type, String data) {
                        if (finished.get() || disposed.get()) return;
                        try {
                            JsonElement parsed = JsonParser.parseString(data);
                            if (!parsed.isJsonObject()) throw new ProtocolException("Expected an SSE JSON object");
                            JsonObject raw = parsed.getAsJsonObject();
                            if (raw.has("error")) { complete(new StreamApiException(response, raw, JSON.getGson().fromJson(raw, VerificationStreamError.class))); return; }
                            if (raw.has("result")) { complete(new Final(raw)); return; }
                            emit(raw.has("stage") ? new Stage(raw) : raw.has("state") ? new Progress(raw) : new Unknown(raw));
                        } catch (RuntimeException error) { complete(new ProtocolException("Malformed verification event")); }
                    }
                    @Override public void onClosed(EventSource source) { complete(new ProtocolException("Verification stream ended before a terminal event")); }
                    @Override public void onFailure(EventSource source, Throwable error, Response value) {
                        if (finished.get() || disposed.get()) return;
                        if (value == null) value = HttpPolicy.responseFromFailure(error);
                        // OkHttp replaces the body with an empty sentinel after opening
                        // an event source; inspect the retained header, not that body.
                        MediaType contentType = value == null || value.header("Content-Type") == null ? null : MediaType.parse(value.header("Content-Type"));
                        if (value != null && value.isSuccessful() && (contentType == null || !contentType.type().equals("text") || !contentType.subtype().equals("event-stream"))) {
                            complete(new ProtocolException("Expected a text/event-stream response body")); return;
                        }
                        if (value != null && !value.isSuccessful()) {
                            Object body = "";
                            try {
                                String text = value.peekBody(65536).string(); body = text;
                                try { body = JsonParser.parseString(text); } catch (RuntimeException ignored) { }
                            } catch (IOException ignored) { }
                            complete(new StreamApiException(value, body, null)); value.close(); return;
                        }
                        complete(error instanceof SocketTimeoutException ? new StreamTimeoutException("idle") : new TransportException());
                    }
                });
            if (finished.get() || disposed.get()) source.cancel();
        }
        // At most one queued event. A slow consumer blocks parsing without accumulating events.
        private void emit(Object value) {
            try { while (!disposed.get() && !finished.get() && !queue.offer(value, 25, TimeUnit.MILLISECONDS)) { } }
            catch (InterruptedException error) { Thread.currentThread().interrupt(); close(); }
        }
        private void complete(Object value) {
            if (!finished.compareAndSet(false, true) || disposed.get()) return;
            synchronized (this) { if (source != null) source.cancel(); if (timeout != null) timeout.cancel(false); }
            terminal = value;
        }
        @Override public boolean hasNext() {
            if (disposed.get()) return false;
            if (next == null) {
                start();
                try {
                    while (next == null) {
                        next = queue.poll(25, TimeUnit.MILLISECONDS);
                        if (next == null && terminal != null) next = terminal;
                    }
                }
                catch (InterruptedException error) { Thread.currentThread().interrupt(); close(); throw new CancellationException("Stream consumer interrupted"); }
            }
            if (next == end) return false;
            if (next instanceof RuntimeException) { RuntimeException error = (RuntimeException) next; close(); throw error; }
            return true;
        }
        @Override public Event next() {
            if (!hasNext()) throw new NoSuchElementException();
            Event value = (Event) next; next = null;
            if (value instanceof Final) close();
            return value;
        }
        /** May be called from another thread to cancel a pending read. */
        @Override public void close() {
            if (!disposed.compareAndSet(false, true)) return;
            synchronized (this) { if (source != null) source.cancel(); if (timeout != null) timeout.cancel(false); }
            queue.clear(); queue.offer(end); streams.remove(this);
        }
    }
}
