import io.reacon.sdk.VerificationStreamClient;
import io.reacon.sdk.VerificationStreamClient.*;
import com.google.gson.JsonElement;
import java.time.Duration;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.*;
import java.util.concurrent.*;

public class Consumer {
    static final Options options = new Options("true", null, Duration.ofSeconds(30), Duration.ofMinutes(5));
    static void check(boolean value, String message) { if (!value) throw new AssertionError(message); }
    static List<Event> collect(VerificationStreamClient client, String scenario, Options settings) {
        List<Event> events = new ArrayList<>();
        try (VerificationStream stream = client.streamVerification(scenario + "@example.test", settings)) {
            for (Event event : stream) events.add(event);
        }
        return events;
    }
    static void success(List<Event> events) {
        check(events.size() == 4 && events.get(0) instanceof Stage && events.get(1) instanceof Unknown && events.get(2) instanceof Progress && events.get(3) instanceof Final, "event classification");
        check(events.get(0).raw.get("label").getAsString().equals("hé🚀"), "split UTF-8");
        check(((Final) events.get(3)).data.getResult().getAcceptsAll() == null && ((Final) events.get(3)).data.getResult().getStatus().equals("future-status"), "typed terminal result");
    }
    public static void main(String[] args) throws Exception {
        String retainedJar = System.getenv("REACON_RETAINED_JAR");
        if (retainedJar != null) {
            String specification = System.getProperty("java.specification.version");
            String major = specification.startsWith("1.") ? specification.substring(2) : specification;
            check(major.equals(System.getenv("REACON_EXPECTED_JAVA_MAJOR")), "Streaming runtime differs");
            String location = VerificationStreamClient.class.getProtectionDomain().getCodeSource().getLocation().getPath();
            check(Arrays.equals(java.nio.file.Files.readAllBytes(java.nio.file.Paths.get(location)),
                java.nio.file.Files.readAllBytes(java.nio.file.Paths.get(retainedJar))), "Streaming SDK JAR differs from retained package");
            com.google.gson.JsonObject proof = new com.google.gson.JsonObject();
            proof.addProperty("runtime", System.getProperty("java.version"));
            proof.addProperty("specificationVersion", specification);
            proof.addProperty("installedJarMatchesRetained", true);
            java.nio.file.Files.write(java.nio.file.Paths.get("/results/streaming-runtime.json"), proof.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8));
        }
        String url = System.getenv("REACON_TEST_URL");
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try (VerificationStreamClient client = new VerificationStreamClient("synthetic-java", url);
             VerificationStreamClient isolated = new VerificationStreamClient("isolated-java", url)) {
            try (VerificationStream unused = client.streamVerification("never@example.test", options)) { }
            Future<List<Event>> first = executor.submit(() -> collect(client, "success", options));
            Future<List<Event>> second = executor.submit(() -> collect(isolated, "isolated", options));
            success(first.get()); success(second.get());
            try { collect(client, "error", options); throw new AssertionError("Missing terminal error"); }
            catch (StreamApiException error) { check(error.status == 200 && error.event.getCode().equals("INSUFFICIENT_CREDITS") && error.event.getRemainingCredits().intValue() == 0 && error.requestId().equals("req-stream"), "terminal error metadata"); }
            String[] scenarios = {"pre402", "pre429", "proxy", "redirect"}; int[] statuses = {402, 429, 502, 307};
            for (int i = 0; i < scenarios.length; i++) {
                try { collect(client, scenarios[i], options); throw new AssertionError("Missing HTTP error"); }
                catch (StreamApiException error) {
                    check(error.status == statuses[i], "HTTP status");
                    if (error.status == 402 || error.status == 429) check(((JsonElement) error.body).getAsJsonObject().get("code").getAsString().equals("FIXTURE_ERROR") && error.requestId().equals("req-stream"), "HTTP metadata");
                    if (error.status == 502) check(error.requestId() == null && error.body instanceof String, "proxy error");
                }
            }
            for (String scenario : Arrays.asList("wrongtype", "malformed", "invalidresult", "eof")) {
                try { collect(client, scenario, options); throw new AssertionError("Missing protocol error: " + scenario); } catch (ProtocolException expected) { }
            }
            try { collect(client, "disconnect", options); throw new AssertionError("Missing transport error"); } catch (TransportException expected) { }
            for (String phase : Arrays.asList("idle", "total", "headers")) {
                try { collect(client, phase, new Options("true", null, Duration.ofMillis(80), Duration.ofMillis(200))); throw new AssertionError("Missing timeout"); }
                catch (StreamTimeoutException error) { if (!phase.equals("headers")) check(error.phase.equals(phase), "timeout phase"); }
            }
            try (VerificationStream stream = client.streamVerification("cancel@example.test", options)) {
                check(stream.next() instanceof Stage, "cancel first event");
                Future<Boolean> pending = executor.submit(() -> stream.hasNext());
                Thread.sleep(20); stream.close(); check(!pending.get(1, TimeUnit.SECONDS), "cancel pending read");
            }
            try (VerificationStream stream = client.streamVerification("early@example.test", options)) { check(stream.next() instanceof Stage, "early first event"); }
            HttpURLConnection control = (HttpURLConnection) new URL(url + "/_assert_closed").openConnection();
            try { check(control.getResponseCode() == 200, "closure while clients remain alive"); } finally { control.disconnect(); }
            System.out.println("Java streaming protocol, cancellation and live closure assertions passed");
        } finally { executor.shutdownNow(); }
    }
}
