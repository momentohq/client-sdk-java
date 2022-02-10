package momento.sdk;

import static momento.sdk.OtelTestHelpers.setOtelSDK;
import static momento.sdk.OtelTestHelpers.startIntegrationTestOtel;
import static momento.sdk.OtelTestHelpers.stopIntegrationTestOtel;
import static momento.sdk.OtelTestHelpers.verifyGetTrace;
import static momento.sdk.OtelTestHelpers.verifySetTrace;
import static momento.sdk.ScsDataTestHelper.assertSetResponse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.opentelemetry.sdk.OpenTelemetrySdk;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import momento.sdk.exceptions.AuthenticationException;
import momento.sdk.exceptions.NotFoundException;
import momento.sdk.exceptions.TimeoutException;
import momento.sdk.messages.CacheGetResponse;
import momento.sdk.messages.CacheGetStatus;
import momento.sdk.messages.CacheSetResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests with Async APIs. */
final class SimpleCacheDataPlaneAsyncTest extends BaseTestClass {

  private static final int DEFAULT_ITEM_TTL_SECONDS = 60;
  private String authToken;
  private String cacheName;

  @BeforeEach
  void setup() {
    authToken = System.getenv("TEST_AUTH_TOKEN");
    cacheName = System.getenv("TEST_CACHE_NAME");
  }

  @AfterEach
  void tearDown() throws Exception {
    stopIntegrationTestOtel();
  }

  @Test
  void getReturnsHitAfterSet() throws Exception {
    runSetAndGetWithHitTest(SimpleCacheClient.builder(authToken, DEFAULT_ITEM_TTL_SECONDS).build());
  }

  @Test
  void getReturnsHitAfterSetWithTracing() throws Exception {
    startIntegrationTestOtel();
    OpenTelemetrySdk openTelemetry = setOtelSDK();

    runSetAndGetWithHitTest(
        new SimpleCacheClient(
            authToken, DEFAULT_ITEM_TTL_SECONDS, Optional.of(openTelemetry), Optional.empty()));

    // To accommodate for delays in tracing logs to appear in docker
    Thread.sleep(1000);
    verifySetTrace("1");
    verifyGetTrace("1");
  }

  @Test
  void cacheMissSuccess() throws Exception {
    runMissTest(SimpleCacheClient.builder(authToken, DEFAULT_ITEM_TTL_SECONDS).build());
  }

  @Test
  void cacheMissSuccessWithTracing() throws Exception {
    startIntegrationTestOtel();
    OpenTelemetrySdk openTelemetry = setOtelSDK();

    runMissTest(
        new SimpleCacheClient(
            authToken, DEFAULT_ITEM_TTL_SECONDS, Optional.of(openTelemetry), Optional.empty()));

    // To accommodate for delays in tracing logs to appear in docker
    Thread.sleep(1000);
    verifySetTrace("0");
    verifyGetTrace("1");
  }

  @Test
  void itemDroppedAfterTtlExpires() throws Exception {
    runTtlTest(SimpleCacheClient.builder(authToken, DEFAULT_ITEM_TTL_SECONDS).build());
  }

  @Test
  void itemDroppedAfterTtlExpiresWithTracing() throws Exception {
    startIntegrationTestOtel();
    OpenTelemetrySdk openTelemetry = setOtelSDK();

    runTtlTest(
        new SimpleCacheClient(
            authToken, DEFAULT_ITEM_TTL_SECONDS, Optional.of(openTelemetry), Optional.empty()));

    // To accommodate for delays in tracing logs to appear in docker
    Thread.sleep(1000);
    verifySetTrace("1");
    verifyGetTrace("1");
  }

  @Test
  void badTokenThrowsAuthenticationException() {
    String badToken =
        "eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJpbnRlZ3JhdGlvbiIsImNwIjoiY29udHJvbC5jZWxsLWFscGhhLWRldi5wcmVwcm9kLmEubW9tZW50b2hxLmNvbSIsImMiOiJjYWNoZS5jZWxsLWFscGhhLWRldi5wcmVwcm9kLmEubW9tZW50b2hxLmNvbSJ9.gdghdjjfjyehhdkkkskskmmls76573jnajhjjjhjdhnndy";
    SimpleCacheClient target =
        SimpleCacheClient.builder(badToken, DEFAULT_ITEM_TTL_SECONDS).build();
    ExecutionException e =
        assertThrows(ExecutionException.class, () -> target.getAsync(cacheName, "").get());
    assertTrue(e.getCause() instanceof AuthenticationException);
  }

  @Test
  public void nonExistentCacheNameThrowsNotFoundOnGetOrSet() {
    SimpleCacheClient target =
        SimpleCacheClient.builder(authToken, DEFAULT_ITEM_TTL_SECONDS).build();
    String cacheName = UUID.randomUUID().toString();

    ExecutionException setException =
        assertThrows(ExecutionException.class, () -> target.getAsync(cacheName, "").get());
    assertTrue(setException.getCause() instanceof NotFoundException);

    ExecutionException getException =
        assertThrows(ExecutionException.class, () -> target.setAsync(cacheName, "", "", 10).get());
    assertTrue(getException.getCause() instanceof NotFoundException);
  }

  @Test
  public void getWithShortTimeoutThrowsException() {
    try (SimpleCacheClient client =
        SimpleCacheClient.builder(authToken, DEFAULT_ITEM_TTL_SECONDS)
            .requestTimeout(Duration.ofMillis(1))
            .build()) {
      ExecutionException e =
          assertThrows(ExecutionException.class, () -> client.getAsync("cache", "key").get());
      assertTrue(e.getCause() instanceof TimeoutException);
    }
  }

  @Test
  public void allowEmptyKeyValues() throws Exception {
    try (SimpleCacheClient client =
        SimpleCacheClient.builder(authToken, DEFAULT_ITEM_TTL_SECONDS).build()) {
      String emptyKey = "";
      String emptyValue = "";
      client.setAsync(cacheName, emptyKey, emptyValue).get();
      CacheGetResponse response = client.getAsync(cacheName, emptyKey).get();
      assertEquals(emptyValue, response.string().get());
    }
  }

  @Test
  public void setWithShortTimeoutThrowsException() {
    try (SimpleCacheClient client =
        SimpleCacheClient.builder(authToken, DEFAULT_ITEM_TTL_SECONDS)
            .requestTimeout(Duration.ofMillis(1))
            .build()) {
      ExecutionException e =
          assertThrows(
              ExecutionException.class, () -> client.setAsync("cache", "key", "value").get());
      assertTrue(e.getCause() instanceof TimeoutException);
    }
  }

  private void runSetAndGetWithHitTest(SimpleCacheClient target) throws Exception {
    String key = UUID.randomUUID().toString();
    String value = UUID.randomUUID().toString();

    // Successful Set
    CompletableFuture<CacheSetResponse> setResponse = target.setAsync(cacheName, key, value);
    assertSetResponse(value, setResponse.get());

    // Successful Get with Hit
    CompletableFuture<CacheGetResponse> getResponse = target.getAsync(cacheName, key);
    assertEquals(CacheGetStatus.HIT, getResponse.get().status());
    assertEquals(value, getResponse.get().string().get());
  }

  private void runTtlTest(SimpleCacheClient target) throws Exception {
    String key = UUID.randomUUID().toString();

    // Set Key sync
    CompletableFuture<CacheSetResponse> setRsp = target.setAsync(cacheName, key, "", 1);

    Thread.sleep(2000);

    // Get Key that was just set
    CompletableFuture<CacheGetResponse> rsp = target.getAsync(cacheName, key);
    assertEquals(CacheGetStatus.MISS, rsp.get().status());
    assertFalse(rsp.get().string().isPresent());
  }

  private void runMissTest(SimpleCacheClient target) throws Exception {
    // Get Key that was just set
    CompletableFuture<CacheGetResponse> rsFuture =
        target.getAsync(cacheName, UUID.randomUUID().toString());

    CacheGetResponse rsp = rsFuture.get();
    assertEquals(CacheGetStatus.MISS, rsp.status());
    assertFalse(rsp.inputStream().isPresent());
    assertFalse(rsp.byteArray().isPresent());
    assertFalse(rsp.byteBuffer().isPresent());
    assertFalse(rsp.string().isPresent());
    assertFalse(rsp.string(Charset.defaultCharset()).isPresent());
  }
}
