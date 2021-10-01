package momento.sdk;

import static momento.sdk.TestHelpers.DEFAULT_CACHE_ENDPOINT;
import static org.junit.jupiter.api.Assertions.*;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import momento.sdk.exceptions.CacheNotFoundException;
import momento.sdk.exceptions.ClientSdkException;
import momento.sdk.exceptions.PermissionDeniedException;
import momento.sdk.messages.CacheGetResponse;
import momento.sdk.messages.CacheSetResponse;
import momento.sdk.messages.MomentoCacheResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

final class CacheTest {

  private Cache cache;

  @BeforeAll
  static void beforeAll() {
    if (System.getenv("TEST_AUTH_TOKEN") == null) {
      throw new IllegalArgumentException(
          "Integration tests require TEST_AUTH_TOKEN env var; see README for more details.");
    }
    if (System.getenv("TEST_CACHE_NAME") == null) {
      throw new IllegalArgumentException(
          "Integration tests require TEST_CACHE_NAME env var; see README for more details.");
    }
  }

  @BeforeEach
  void setUp() {
    cache = getCache(Optional.empty());
  }

  Cache getCache(Optional<OpenTelemetry> openTelemetry) {
    return getCache(
        System.getenv("TEST_AUTH_TOKEN"), System.getenv("TEST_CACHE_NAME"), openTelemetry);
  }

  Cache getCache(String authToken, String cacheName, Optional<OpenTelemetry> openTelemetry) {
    String endpoint = System.getenv("TEST_ENDPOINT");
    if (endpoint == null) {
      endpoint = DEFAULT_CACHE_ENDPOINT;
    }

    return new Cache(
        authToken, cacheName, openTelemetry, endpoint, System.getenv("TEST_SSL_INSECURE") != null);
  }

  @AfterEach
  void tearDown() throws Exception {
    stopIntegrationTestOtel();
  }

  @Test
  void testBlockingClientHappyPath() {
    testHappyPath(cache);
  }

  @Test
  void testBlockingClientHappyPathWithTracing() throws Exception {
    startIntegrationTestOtel();
    OpenTelemetrySdk openTelemetry = setOtelSDK();
    Cache client = getCache(Optional.of(openTelemetry));
    testHappyPath(client);
    // To accommodate for delays in tracing logs to appear in docker
    Thread.sleep(1000);
    verifySetTrace("1");
    verifyGetTrace("1");
  }

  private static void testHappyPath(Cache cache) {
    String key = UUID.randomUUID().toString();

    // Set Key sync
    CacheSetResponse setRsp =
        cache.set(key, ByteBuffer.wrap("bar".getBytes(StandardCharsets.UTF_8)), 2);
    assertEquals(MomentoCacheResult.Ok, setRsp.getResult());

    // Get Key that was just set
    CacheGetResponse rsp = cache.get(key);
    assertEquals(MomentoCacheResult.Hit, rsp.getResult());
    assertEquals("bar", rsp.asStringUtf8());
  }

  @Test
  void testAsyncClientHappyPath() throws Exception {
    testAsyncHappyPath(cache);
  }

  @Test
  void testAsyncClientHappyPathWithTracing() throws Exception {
    startIntegrationTestOtel();
    OpenTelemetrySdk openTelemetry = setOtelSDK();
    Cache client = getCache(Optional.of(openTelemetry));
    testAsyncHappyPath(client);
    // To accommodate for delays in tracing logs to appear in docker
    Thread.sleep(1000);
    verifySetTrace("1");
    verifyGetTrace("1");
  }

  private static void testAsyncHappyPath(Cache client) throws Exception {
    String key = UUID.randomUUID().toString();
    // Set Key Async
    CompletionStage<CacheSetResponse> setRsp =
        client.setAsync(key, ByteBuffer.wrap("bar".getBytes(StandardCharsets.UTF_8)), 10);
    assertEquals(MomentoCacheResult.Ok, setRsp.toCompletableFuture().get().getResult());

    // Get Key Async
    CacheGetResponse rsp = client.getAsync(key).toCompletableFuture().get();

    assertEquals(MomentoCacheResult.Hit, rsp.getResult());
    assertEquals("bar", rsp.asStringUtf8());
  }

  @Test
  void testTtlHappyPath() throws Exception {
    testTtlHappyPath(cache);
  }

  @Test
  void testTtlHappyPathWithTracing() throws Exception {
    startIntegrationTestOtel();
    OpenTelemetrySdk openTelemetry = setOtelSDK();
    Cache client = getCache(Optional.of(openTelemetry));
    testTtlHappyPath(client);
    // To accommodate for delays in tracing logs to appear in docker
    Thread.sleep(1000);
    verifySetTrace("1");
    verifyGetTrace("1");
  }

  private static void testTtlHappyPath(Cache client) throws Exception {
    String key = UUID.randomUUID().toString();

    // Set Key sync
    CacheSetResponse setRsp =
        client.set(key, ByteBuffer.wrap("bar".getBytes(StandardCharsets.UTF_8)), 1);
    assertEquals(MomentoCacheResult.Ok, setRsp.getResult());

    Thread.sleep(1500);

    // Get Key that was just set
    CacheGetResponse rsp = client.get(key);
    assertEquals(MomentoCacheResult.Miss, rsp.getResult());
  }

  @Test
  void testMissHappyPath() {
    testMissHappyPathInternal(cache);
  }

  @Test
  void testMissHappyPathWithTracing() throws Exception {
    startIntegrationTestOtel();
    OpenTelemetrySdk openTelemetry = setOtelSDK();
    Cache client = getCache(Optional.of(openTelemetry));
    testMissHappyPathInternal(client);
    // To accommodate for delays in tracing logs to appear in docker
    Thread.sleep(1000);
    verifySetTrace("0");
    verifyGetTrace("1");
  }

  private static void testMissHappyPathInternal(Cache client) {
    // Get Key that was just set
    CacheGetResponse rsp = client.get(UUID.randomUUID().toString());

    assertEquals(MomentoCacheResult.Miss, rsp.getResult());
  }

  @Test
  void testBadAuthToken() {
    Cache badCredClient = getCache("BAD_TOKEN", "dummy", Optional.empty());
    testBadAuthToken(badCredClient);
  }

  @Test
  void testBadAuthTokenWithTracing() throws Exception {
    startIntegrationTestOtel();
    OpenTelemetrySdk openTelemetry = setOtelSDK();
    Cache client = getCache("BAD_TOKEN", "dummy", Optional.of(openTelemetry));
    testBadAuthToken(client);
    // To accommodate for delays in tracing logs to appear in docker
    Thread.sleep(1000);
    verifySetTrace("1");
    verifyGetTrace("1");
  }

  private static void testBadAuthToken(Cache badCredClient) {
    // Bad Auth for Get
    assertThrows(PermissionDeniedException.class, () -> badCredClient.get("myCacheKey"));

    // Bad Auth for Set
    assertThrows(
        PermissionDeniedException.class,
        () ->
            badCredClient.set(
                "myCacheKey",
                ByteBuffer.wrap("cache me if you can".getBytes(StandardCharsets.UTF_8)),
                500));
  }

  @Test
  public void unreachableEndpoint_ThrowsException() {
    Cache cache =
        new Cache(
            System.getenv("TEST_AUTH_TOKEN"),
            System.getenv("TEST_CACHE_NAME"),
            "nonexistent.preprod.a.momentohq.com");

    assertThrows(ClientSdkException.class, () -> cache.get("key"));
  }

  @Disabled("TODO: Update to catch cache not ready and then do a get again to see not found.")
  @Test
  public void invalidCache_ThrowsNotFoundException() {
    Cache cache =
        getCache(System.getenv("TEST_AUTH_TOKEN"), UUID.randomUUID().toString(), Optional.empty());

    assertThrows(CacheNotFoundException.class, () -> cache.get("key"));
  }

  @Test
  public void setAndGetWithByteKeyValuesMustSucceed() {
    byte[] key = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
    byte[] value = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);

    CacheSetResponse setResponse = cache.set(key, value, 3);
    assertEquals(setResponse.getResult(), MomentoCacheResult.Ok);

    CacheGetResponse getResponse = cache.get(key);
    assertEquals(getResponse.getResult(), MomentoCacheResult.Hit);
    assertArrayEquals(value, getResponse.asByteArray());
  }
  /** ================ HELPER FUNCTIONS ====================================== */
  OpenTelemetrySdk setOtelSDK() {
    String otelGwUrl = "0.0.0.0";
    // this is due to the cfn export format we are using for the vpc endpoints
    String serviceUrl = "http://" + otelGwUrl + ":" + "4317";
    OtlpGrpcSpanExporter spanExporter =
        OtlpGrpcSpanExporter.builder()
            .setEndpoint(serviceUrl)
            .setTimeout(2, TimeUnit.SECONDS)
            .build();
    BatchSpanProcessor spanProcessor =
        BatchSpanProcessor.builder(spanExporter).setScheduleDelay(1, TimeUnit.MILLISECONDS).build();
    SdkTracerProvider sdkTracerProvider =
        SdkTracerProvider.builder().addSpanProcessor(spanProcessor).build();
    OpenTelemetrySdk openTelemetry =
        OpenTelemetrySdk.builder()
            .setTracerProvider(sdkTracerProvider)
            .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
            .build();
    Runtime.getRuntime().addShutdownHook(new Thread(sdkTracerProvider::shutdown));
    return openTelemetry;
  }

  void startIntegrationTestOtel() throws Exception {
    stopIntegrationTestOtel();
    shellex(
        "docker run -p 4317:4317 --name integtest_otelcol "
            + "    -v ${PWD}/otel-collector-config.yaml:/etc/otel-collector-config.yaml "
            + "    -d otel/opentelemetry-collector:latest "
            + "    --config=/etc/otel-collector-config.yaml",
        true,
        "started local otel container for integration testing",
        "failed to bootstrap integration test local otel container");
    shellExpect(
        5,
        "docker logs integtest_otelcol 2>& 1",
        ".*Everything is ready. Begin running and processing data.*");
  }

  void stopIntegrationTestOtel() throws Exception {
    shellex(
        "docker stop integtest_otelcol && docker rm integtest_otelcol",
        false,
        "successfully stopped otelcol test container",
        "it is okay for this to fail because maybe it is not present");
  }

  void verifySetTrace(String expectedCount) throws Exception {
    String count =
        shellex(
            "docker logs   integtest_otelcol 2>& 1 | grep  Name | grep java-sdk-set-request | wc -l",
            true,
            "Verify set trace",
            "failed to verify set trace");
    assertEquals(expectedCount, count.trim());
  }

  void verifyGetTrace(String expectedCount) throws Exception {
    String count =
        shellex(
            "docker logs   integtest_otelcol 2>& 1 | grep  Name | grep java-sdk-get-request | wc -l",
            true,
            "Verify get trace",
            "failed to verify get trace");
    assertEquals(expectedCount, count.trim());
  }

  // Polls a command until the expected result comes back
  private void shellExpect(double timeoutSeconds, String command, String outputRegex)
      throws Exception {
    long start = System.currentTimeMillis();
    String lastOutput = "";

    while ((System.currentTimeMillis() - start) / 1000.0 < timeoutSeconds) {
      lastOutput = shellex(command, false, null, null);
      if (lastOutput.matches(outputRegex)) {
        return;
      }
    }

    throw new InternalError(String.format("Never got expected output. Last:\n%s", lastOutput));
  }

  private String shellex(
      String command, Boolean expectSuccess, String successDescription, String failDescription)
      throws Exception {
    ProcessBuilder processBuilder = new ProcessBuilder();
    processBuilder.command("sh", "-c", command);
    Process process = processBuilder.start();

    int exitCode = process.waitFor();

    String output = "";
    try (BufferedReader reader =
        new BufferedReader(new InputStreamReader(process.getInputStream()))) {

      String line;
      while ((line = reader.readLine()) != null) {
        System.out.println(line);
        output += line;
      }
    }

    if (exitCode == 0) {
      if (successDescription != null) {
        System.out.println(successDescription);
      }
    } else if (expectSuccess) {
      throw new InternalError(failDescription + "exit_code:" + exitCode);
    } else {
      if (failDescription != null) {
        System.out.println(failDescription);
      }
    }

    return output;
  }
}
