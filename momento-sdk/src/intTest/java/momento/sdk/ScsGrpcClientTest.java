package momento.sdk;

import static momento.sdk.TestHelpers.DEFAULT_CACHE_ENDPOINT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
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
import org.junit.jupiter.api.Test;

final class ScsGrpcClientTest {

  private static final int DEFAULT_ITEM_TTL_SECONDS = 60;
  private String authToken;
  private String cacheName;
  private String endpoint;

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
  void setup() {
    authToken = System.getenv("TEST_AUTH_TOKEN");
    cacheName = System.getenv("TEST_CACHE_NAME");
    endpoint = DEFAULT_CACHE_ENDPOINT;
  }

  @AfterEach
  void tearDown() throws Exception {
    stopIntegrationTestOtel();
  }

  @Test
  void getReturnsHitAfterSet() throws Exception {
    runSetAndGetWithHitTest(new ScsGrpcClient(authToken, endpoint));
  }

  @Test
  void getReturnsHitAfterSetWithTracing() throws Exception {
    startIntegrationTestOtel();
    OpenTelemetrySdk openTelemetry = setOtelSDK();

    runSetAndGetWithHitTest(new ScsGrpcClient(authToken, endpoint, Optional.of(openTelemetry)));

    // To accommodate for delays in tracing logs to appear in docker
    Thread.sleep(1000);
    verifySetTrace("1");
    verifyGetTrace("1");
  }

  @Test
  void cacheMissSuccess() throws Exception {
    runMissTest(new ScsGrpcClient(authToken, endpoint));
  }

  @Test
  void cacheMissSuccessWithTracing() throws Exception {
    startIntegrationTestOtel();
    OpenTelemetrySdk openTelemetry = setOtelSDK();

    runMissTest(new ScsGrpcClient(authToken, endpoint, Optional.of(openTelemetry)));

    // To accommodate for delays in tracing logs to appear in docker
    Thread.sleep(1000);
    verifySetTrace("0");
    verifyGetTrace("1");
  }

  @Test
  void itemDroppedAfterTtlExpires() throws Exception {
    runTtlTest(new ScsGrpcClient(authToken, endpoint));
  }

  @Test
  void itemDroppedAfterTtlExpiresWithTracing() throws Exception {
    startIntegrationTestOtel();
    OpenTelemetrySdk openTelemetry = setOtelSDK();

    runTtlTest(new ScsGrpcClient(authToken, endpoint, Optional.of(openTelemetry)));

    // To accommodate for delays in tracing logs to appear in docker
    Thread.sleep(1000);
    verifySetTrace("1");
    verifyGetTrace("1");
  }

  @Test
  void badTokenThrowsPermissionDenied() {
    ScsGrpcClient target = new ScsGrpcClient("bad_token", endpoint);
    ExecutionException e =
        assertThrows(
            ExecutionException.class, () -> target.sendGet(cacheName, ByteString.EMPTY).get());
    assertTrue(e.getCause() instanceof PermissionDeniedException);
  }

  @Test
  public void unreachableEndpointThrowsException() {
    ScsGrpcClient target = new ScsGrpcClient(authToken, "unknown.momentohq.com");
    ExecutionException e =
        assertThrows(
            ExecutionException.class, () -> target.sendGet(cacheName, ByteString.EMPTY).get());
    assertTrue(e.getCause() instanceof ClientSdkException);
  }

  @Test
  public void nonExistentCacheNameThrowsNotFoundOnGetOrSet() {
    ScsGrpcClient target = new ScsGrpcClient(authToken, endpoint);
    String cacheName = UUID.randomUUID().toString();

    ExecutionException setException =
        assertThrows(
            ExecutionException.class, () -> target.sendGet(cacheName, ByteString.EMPTY).get());
    assertTrue(setException.getCause() instanceof CacheNotFoundException);

    ExecutionException getException =
        assertThrows(
            ExecutionException.class,
            () -> target.sendSet(cacheName, ByteString.EMPTY, ByteString.EMPTY, 10).get());
    assertTrue(getException.getCause() instanceof CacheNotFoundException);
  }

  private void runSetAndGetWithHitTest(ScsGrpcClient target) throws Exception {
    ByteString key = ByteString.copyFromUtf8(UUID.randomUUID().toString());
    ByteString value = ByteString.copyFromUtf8(UUID.randomUUID().toString());

    // Successful Set
    CompletableFuture<CacheSetResponse> setResponse =
        target.sendSet(cacheName, key, value, DEFAULT_ITEM_TTL_SECONDS);
    assertEquals(MomentoCacheResult.Ok, setResponse.get().result());

    // Successful Get with Hit
    CompletableFuture<CacheGetResponse> getResponse = target.sendGet(cacheName, key);
    assertEquals(MomentoCacheResult.Hit, getResponse.get().result());
    assertEquals(value.toStringUtf8(), getResponse.get().string().get());
  }

  private void runTtlTest(ScsGrpcClient target) throws Exception {
    ByteString key = ByteString.copyFromUtf8(UUID.randomUUID().toString());

    // Set Key sync
    CompletableFuture<CacheSetResponse> setRsp =
        target.sendSet(cacheName, key, ByteString.EMPTY, 1);
    assertEquals(MomentoCacheResult.Ok, setRsp.get().result());

    Thread.sleep(1500);

    // Get Key that was just set
    CompletableFuture<CacheGetResponse> rsp = target.sendGet(cacheName, key);
    assertEquals(MomentoCacheResult.Miss, rsp.get().result());
    assertFalse(rsp.get().string().isPresent());
  }

  private void runMissTest(ScsGrpcClient target) throws Exception {
    // Get Key that was just set
    CompletableFuture<CacheGetResponse> rsFuture =
        target.sendGet(cacheName, ByteString.copyFromUtf8(UUID.randomUUID().toString()));

    CacheGetResponse rsp = rsFuture.get();
    assertEquals(MomentoCacheResult.Miss, rsp.result());
    assertFalse(rsp.inputStream().isPresent());
    assertFalse(rsp.byteArray().isPresent());
    assertFalse(rsp.byteBuffer().isPresent());
    assertFalse(rsp.string().isPresent());
    assertFalse(rsp.string(Charset.defaultCharset()).isPresent());
  }

  private OpenTelemetrySdk setOtelSDK() {
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

  private void startIntegrationTestOtel() throws Exception {
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

  private void stopIntegrationTestOtel() throws Exception {
    shellex(
        "docker stop integtest_otelcol && docker rm integtest_otelcol",
        false,
        "successfully stopped otelcol test container",
        "it is okay for this to fail because maybe it is not present");
  }

  private void verifySetTrace(String expectedCount) throws Exception {
    String count =
        shellex(
            "docker logs   integtest_otelcol 2>& 1 | grep  Name | grep java-sdk-set-request | wc -l",
            true,
            "Verify set trace",
            "failed to verify set trace");
    assertEquals(expectedCount, count.trim());
  }

  private void verifyGetTrace(String expectedCount) throws Exception {
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
