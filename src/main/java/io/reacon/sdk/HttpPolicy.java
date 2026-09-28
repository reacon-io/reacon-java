// Copyright Reacon contributors. Licensed under Apache-2.0.
package io.reacon.sdk;

import com.google.gson.*;
import com.google.gson.annotations.SerializedName;
import com.google.gson.internal.bind.TypeAdapters;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import okhttp3.*;
import okio.*;
import java.io.*;
import java.lang.reflect.*;
import java.nio.ByteBuffer;
import java.nio.charset.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/** Network and decoding policy shared by every generated JSON/CSV operation. */
public final class HttpPolicy {
    private HttpPolicy() {}
    public static class RequestTimeoutException extends ApiException {
        RequestTimeoutException(Throwable cause) { super("Reacon request deadline exceeded", cause, 0, null); }
    }
    public static class RequestCancelledException extends ApiException {
        RequestCancelledException(Throwable cause) { super("Reacon request cancelled", cause, 0, null); }
    }
    public static class TransportException extends ApiException {
        TransportException(Throwable cause) { super("Reacon transport failed", cause, 0, null); }
    }
    public static class ResponseException extends ApiException {
        private final byte[] bytes;
        ResponseException(String message, Throwable cause, int status, Map<String,List<String>> headers, byte[] bytes) {
            super(message, cause, status, headers, new String(bytes, StandardCharsets.UTF_8)); this.bytes = bytes.clone();
        }
        public byte[] getResponseBytes() { return bytes.clone(); }
        public String getRequestId() {
            for (Map.Entry<String,List<String>> entry : getResponseHeaders().entrySet())
                if (entry.getKey().equalsIgnoreCase("x-request-id") && !entry.getValue().isEmpty()) return entry.getValue().get(0);
            return null;
        }
        public JsonElement getParsedBody() { try { return parse(bytes); } catch (Exception error) { return null; } }
        public String getErrorCode() {
            JsonElement body = getParsedBody(); if (body == null || !body.isJsonObject()) return null;
            JsonObject obj = body.getAsJsonObject(); JsonElement code = obj.get("code");
            if (code == null && obj.has("error") && obj.get("error").isJsonObject()) code = obj.getAsJsonObject("error").get("code");
            return code != null && code.isJsonPrimitive() && code.getAsJsonPrimitive().isString() ? code.getAsString() : null;
        }
    }
    public static class ResponseDecodeException extends ResponseException {
        ResponseDecodeException(Throwable cause, Response response, byte[] bytes) {
            super("Unable to decode Reacon response", cause, response.code(), response.headers().toMultimap(), bytes);
        }
    }
    private static final class StatusIOException extends IOException {
        final ResponseException error;
        final Response response;
        StatusIOException(Response response, byte[] bytes) {
            super("Reacon returned HTTP " + response.code());
            error = new ResponseException(getMessage(), null, response.code(), response.headers().toMultimap(), bytes);
            this.response = response.newBuilder().body(ResponseBody.create(response.body() == null ? null : response.body().contentType(), bytes)).build();
        }
    }
    /** Retained response for the streaming listener when an implicit status retry was stopped. */
    static Response responseFromFailure(Throwable error) {
        return error instanceof StatusIOException ? ((StatusIOException) error).response : null;
    }
    /** Derives a client while retaining its pool, dispatcher, TLS/proxy configuration and interceptors. */
    public static OkHttpClient client(OkHttpClient client) {
        OkHttpClient.Builder builder = client.newBuilder().retryOnConnectionFailure(false)
            .followRedirects(false).followSslRedirects(false).authenticator(Authenticator.NONE)
            .proxyAuthenticator(Authenticator.NONE).protocols(Collections.singletonList(Protocol.HTTP_1_1))
            .callTimeout(30, TimeUnit.SECONDS);
        boolean installed = false;
        for (Interceptor interceptor : builder.networkInterceptors()) if (interceptor instanceof NoStatusRetry) installed = true;
        if (!installed) builder.addNetworkInterceptor(new NoStatusRetry());
        return builder.build();
    }
    private static final class NoStatusRetry implements Interceptor {
        @Override public Response intercept(Chain chain) throws IOException {
            Response response = chain.proceed(chain.request());
            // OkHttp 4.12 retries 503 + Retry-After: 0 independently of
            // retryOnConnectionFailure. Preserve the complete original response.
            if (response.code() == 503 && response.header("Retry-After", "").matches("0+")) {
                try (Response owned = response) { throw new StatusIOException(owned, owned.body() == null ? new byte[0] : owned.body().bytes()); }
            }
            return response;
        }
    }
    public static int timeoutMillis(Duration duration) {
        if (duration == null || duration.isNegative() || duration.isZero()) throw new IllegalArgumentException("Positive request timeout required");
        final long millis;
        try { millis = duration.toMillis(); } catch (ArithmeticException error) { throw new IllegalArgumentException("Request timeout is too large", error); }
        if (millis < 1 || millis > Integer.MAX_VALUE) throw new IllegalArgumentException("Request timeout must be 1..2147483647 milliseconds");
        return (int) millis;
    }
    public static Call call(OkHttpClient client, Request request) { return new DeadlineCall(client.newCall(request)); }
    public static ApiException failure(Call call, IOException error) {
        if (error instanceof StatusIOException) return ((StatusIOException) error).error;
        if (call instanceof DeadlineCall && ((DeadlineCall) call).expired.get()) return new RequestTimeoutException(error);
        if (call instanceof DeadlineCall && ((DeadlineCall) call).cancelled.get()) return new RequestCancelledException(error);
        if (Thread.currentThread().isInterrupted()) return new RequestCancelledException(error);
        if (error instanceof InterruptedIOException) return new RequestTimeoutException(error);
        if (call != null && call.isCanceled()) return new RequestCancelledException(error);
        return new TransportException(error);
    }
    public static <T> ApiResponse<T> execute(Call call, Type type) throws ApiException {
        try (Response response = call.execute()) {
            return new ApiResponse<T>(response.code(), response.headers().toMultimap(), read(call, response, type, false));
        } catch (IOException error) { throw failure(call, error); }
    }
    public static String executeCsv(Call call) throws ApiException {
        try (Response response = call.execute()) { return read(call, response, String.class, true); }
        catch (IOException error) { throw failure(call, error); }
    }
    public static <T> void executeAsync(final Call call, final Type type, final ApiCallback<T> callback) {
        if (callback == null) throw new IllegalArgumentException("callback is required");
        call.enqueue(new Callback() {
            @Override public void onFailure(Call ignored, IOException error) { ApiException failure = failure(call, error); callback.onFailure(failure, failure.getCode(), failure.getResponseHeaders()); }
            @Override public void onResponse(Call ignored, Response response) {
                T value;
                try (Response owned = response) { value = read(call, owned, type, false); }
                catch (ApiException error) { callback.onFailure(error, error.getCode(), error.getResponseHeaders()); return; }
                callback.onSuccess(value, response.code(), response.headers().toMultimap());
            }
        });
    }
    @SuppressWarnings("unchecked")
    public static <T> T read(Call call, Response response, Type type, boolean csv) throws ApiException {
        final byte[] bytes;
        try { bytes = response.body() == null ? new byte[0] : response.body().bytes(); }
        catch (IOException error) { throw failure(call, error); }
        if (!response.isSuccessful()) throw new ResponseException("Reacon returned HTTP " + response.code(), null, response.code(), response.headers().toMultimap(), bytes);
        if (type == null) return null;
        try {
            String media = response.header("Content-Type", "").split(";", 2)[0].trim().toLowerCase(Locale.ROOT);
            if (csv) {
                if (!media.equals("text/csv")) throw new IOException("Expected a CSV response");
                return (T) utf8(bytes);
            }
            if (!media.equals("application/json") && !media.matches("application/[a-z0-9!#$&^_.+-]+\\+json")) throw new IOException("Expected a JSON response");
            JsonElement json = parse(bytes); validate(json, type);
            return JSON.getGson().fromJson(json, type);
        } catch (Exception error) { throw new ResponseDecodeException(error, response, bytes); }
    }
    private static String utf8(byte[] bytes) throws CharacterCodingException {
        return StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT).onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(bytes)).toString();
    }
    private static JsonElement parse(byte[] bytes) throws IOException {
        JsonReader reader = new JsonReader(new StringReader(utf8(bytes))); reader.setLenient(false);
        JsonElement value = TypeAdapters.JSON_ELEMENT.read(reader);
        if (reader.peek() != JsonToken.END_DOCUMENT) throw new IOException("Trailing JSON value");
        return value;
    }
    private static void validate(JsonElement value, Type type) throws IOException {
        if (type == Object.class) return;
        if (type instanceof ParameterizedType) {
            ParameterizedType parameter = (ParameterizedType) type;
            if (parameter.getRawType() instanceof Class && Collection.class.isAssignableFrom((Class<?>) parameter.getRawType())) {
                if (!value.isJsonArray()) throw new IOException("Expected a JSON array");
                for (JsonElement item : value.getAsJsonArray()) validate(item, parameter.getActualTypeArguments()[0]);
                return;
            }
            if (parameter.getRawType() instanceof Class && Map.class.isAssignableFrom((Class<?>) parameter.getRawType())) {
                if (!value.isJsonObject()) throw new IOException("Expected a JSON object");
                for (Map.Entry<String,JsonElement> item : value.getAsJsonObject().entrySet()) validate(item.getValue(), parameter.getActualTypeArguments()[1]);
                return;
            }
        }
        if (!(type instanceof Class)) return;
        Class<?> cls = (Class<?>) type;
        if (cls == Boolean.class || cls == boolean.class) {
            if (!value.isJsonPrimitive() || !value.getAsJsonPrimitive().isBoolean()) throw new IOException("Expected a boolean"); return;
        }
        if (Number.class.isAssignableFrom(cls) || cls.isPrimitive()) {
            if (!value.isJsonPrimitive() || !value.getAsJsonPrimitive().isNumber()) throw new IOException("Expected a number");
            if ((cls == Integer.class || cls == Long.class) && value.getAsBigDecimal().stripTrailingZeros().scale() > 0) throw new IOException("Expected an integer");
            return;
        }
        if (cls == String.class || cls.isEnum()) {
            if (!value.isJsonPrimitive() || !value.getAsJsonPrimitive().isString()) throw new IOException("Expected a string"); return;
        }
        if (!cls.getName().startsWith("io.reacon.sdk.model.")) return;
        if (!value.isJsonObject()) throw new IOException("Expected a model object");
        for (Field field : cls.getDeclaredFields()) {
            SerializedName name = field.getAnnotation(SerializedName.class); if (name == null) continue;
            JsonElement child = value.getAsJsonObject().get(name.value());
            if (child != null && !child.isJsonNull()) validate(child, field.getGenericType());
        }
    }
    private static final ScheduledThreadPoolExecutor TIMERS = new ScheduledThreadPoolExecutor(1, runnable -> {
        Thread thread = new Thread(runnable, "reacon-request-deadlines"); thread.setDaemon(true); return thread;
    });
    // Never execute user callbacks on the timer thread, or create a thread per expired queued call.
    private static final ExecutorService CALLBACKS = Executors.newFixedThreadPool(2, runnable -> {
        Thread thread = new Thread(runnable, "reacon-request-cancellation"); thread.setDaemon(true); return thread;
    });
    static { TIMERS.setRemoveOnCancelPolicy(true); }
    /** One deadline from execute/enqueue through body closure, including dispatcher queue time. */
    private static final class DeadlineCall implements Call {
        final Call delegate;
        final AtomicBoolean expired = new AtomicBoolean(), cancelled = new AtomicBoolean(), callbackStarted = new AtomicBoolean();
        private final AtomicBoolean begun = new AtomicBoolean();
        private volatile ScheduledFuture<?> timer;
        private volatile Callback callback;
        DeadlineCall(Call delegate) { this.delegate = delegate; }
        private void begin() {
            if (!begun.compareAndSet(false, true)) throw new IllegalStateException("Already Executed");
            long remaining = timeout().timeoutNanos();
            if (remaining <= 0) throw new IllegalArgumentException("A positive call timeout is required");
            if (timeout().hasDeadline()) remaining = Math.min(remaining, timeout().deadlineNanoTime() - System.nanoTime());
            if (remaining <= 0) { expired.set(true); delegate.cancel(); signal(new InterruptedIOException("Reacon request deadline exceeded")); return; }
            timer = TIMERS.schedule(() -> { expired.set(true); delegate.cancel(); signal(new InterruptedIOException("Reacon request deadline exceeded")); }, Math.max(0, remaining), TimeUnit.NANOSECONDS);
        }
        private void finish() { ScheduledFuture<?> pending = timer; if (pending != null) pending.cancel(false); }
        private void signal(IOException error) {
            Callback target = callback;
            if (target != null && callbackStarted.compareAndSet(false, true)) CALLBACKS.execute(() -> target.onFailure(this, error));
        }
        private Response wrap(Response response) {
            final ResponseBody body = response.body();
            if (body == null) { finish(); return response; }
            BufferedSource source = Okio.buffer(new ForwardingSource(body.source()) {
                @Override public long read(Buffer sink, long count) throws IOException {
                    try { long read = super.read(sink, count); if (read == -1) finish(); return read; }
                    catch (IOException error) { finish(); throw error; }
                }
                @Override public void close() throws IOException { try { super.close(); } finally { finish(); } }
            });
            return response.newBuilder().body(new ResponseBody() {
                @Override public MediaType contentType() { return body.contentType(); }
                @Override public long contentLength() { return body.contentLength(); }
                @Override public BufferedSource source() { return source; }
            }).build();
        }
        @Override public Response execute() throws IOException {
            begin();
            try { return wrap(delegate.execute()); } catch (IOException | RuntimeException error) { finish(); throw error; }
        }
        @Override public void enqueue(Callback callback) {
            if (callback == null) throw new IllegalArgumentException("callback is required");
            this.callback = callback; begin();
            try {
                delegate.enqueue(new Callback() {
                    @Override public void onFailure(Call ignored, IOException error) {
                        finish(); if (callbackStarted.compareAndSet(false, true)) callback.onFailure(DeadlineCall.this, error);
                    }
                    @Override public void onResponse(Call ignored, Response response) throws IOException {
                        if (!callbackStarted.compareAndSet(false, true)) { response.close(); return; }
                        try { callback.onResponse(DeadlineCall.this, wrap(response)); }
                        catch (IOException | RuntimeException error) { response.close(); finish(); throw error; }
                    }
                });
            } catch (RuntimeException error) { finish(); throw error; }
        }
        @Override public void cancel() { cancelled.set(true); delegate.cancel(); finish(); signal(new IOException("Reacon request cancelled")); }
        @Override public boolean isCanceled() { return delegate.isCanceled(); }
        @Override public boolean isExecuted() { return begun.get(); }
        @Override public Request request() { return delegate.request(); }
        @Override public Timeout timeout() { return delegate.timeout(); }
        @Override public Call clone() {
            Call copy = delegate.clone(); copy.timeout().timeout(timeout().timeoutNanos(), TimeUnit.NANOSECONDS);
            if (timeout().hasDeadline()) copy.timeout().deadlineNanoTime(timeout().deadlineNanoTime());
            return new DeadlineCall(copy);
        }
    }
}
