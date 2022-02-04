package momento.sdk;

import static momento.sdk.OtelTestHelpers.setOtelSDK;
import static momento.sdk.OtelTestHelpers.startIntegrationTestOtel;
import static momento.sdk.OtelTestHelpers.stopIntegrationTestOtel;
import static momento.sdk.OtelTestHelpers.verifyGetTrace;
import static momento.sdk.OtelTestHelpers.verifySetTrace;
import static momento.sdk.ScsDataTestHelper.assertSetResponse;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.opentelemetry.sdk.OpenTelemetrySdk;
import java.io.IOException;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import momento.sdk.exceptions.AuthenticationException;
import momento.sdk.exceptions.NotFoundException;
import momento.sdk.exceptions.TimeoutException;
import momento.sdk.messages.CacheGetResponse;
import momento.sdk.messages.CacheGetStatus;
import momento.sdk.messages.CacheSetResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests with Blocking APIs. */
final class SimpleCacheDataPlaneBlockingTest extends BaseTestClass {

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
  void getReturnsHitAfterSet() throws IOException {
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
  void cacheMissSuccess() {
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
  public void badTokenThrowsAuthenticationException() {
    String badToken =
        "eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJpbnRlZ3JhdGlvbiIsImNwIjoiY29udHJvbC5jZWxsLWFscGhhLWRldi5wcmVwcm9kLmEubW9tZW50b2hxLmNvbSIsImMiOiJjYWNoZS5jZWxsLWFscGhhLWRldi5wcmVwcm9kLmEubW9tZW50b2hxLmNvbSJ9.gdghdjjfjyehhdkkkskskmmls76573jnajhjjjhjdhnndy";
    SimpleCacheClient target =
        SimpleCacheClient.builder(badToken, DEFAULT_ITEM_TTL_SECONDS).build();
    assertThrows(AuthenticationException.class, () -> target.get(cacheName, ""));
  }

  @Test
  public void nonExistentCacheNameThrowsNotFoundOnGetOrSet() {
    SimpleCacheClient target =
        SimpleCacheClient.builder(authToken, DEFAULT_ITEM_TTL_SECONDS).build();
    String cacheName = UUID.randomUUID().toString();

    assertThrows(NotFoundException.class, () -> target.get(cacheName, ""));

    assertThrows(NotFoundException.class, () -> target.set(cacheName, "", "", 10));
  }

  @Test
  public void setAndGetWithByteKeyValuesMustSucceed() {
    byte[] key = {0x01, 0x02, 0x03, 0x04};
    byte[] value = {0x05, 0x06, 0x07, 0x08};
    SimpleCacheClient cache =
        SimpleCacheClient.builder(authToken, DEFAULT_ITEM_TTL_SECONDS).build();
    CacheSetResponse setResponse = cache.set(cacheName, key, value, 60);

    CacheGetResponse getResponse = cache.get(cacheName, key);
    assertEquals(CacheGetStatus.HIT, getResponse.status());
    assertArrayEquals(value, getResponse.byteArray().get());
  }

  @Test
  public void getWithShortTimeoutThrowsException() {
    try (SimpleCacheClient client =
        SimpleCacheClient.builder(authToken, DEFAULT_ITEM_TTL_SECONDS)
            .requestTimeout(Duration.ofMillis(1))
            .build()) {
      assertThrows(TimeoutException.class, () -> client.get("cache", "key"));
    }
  }

  @Test
  public void setWithShortTimeoutThrowsException() {
    try (SimpleCacheClient client =
        SimpleCacheClient.builder(authToken, DEFAULT_ITEM_TTL_SECONDS)
            .requestTimeout(Duration.ofMillis(1))
            .build()) {
      assertThrows(TimeoutException.class, () -> client.set("cache", "key", "value"));
    }
  }

  private void runSetAndGetWithHitTest(SimpleCacheClient target) throws IOException {
    String key = UUID.randomUUID().toString();
    String value = UUID.randomUUID().toString();

    // Successful Set
    CacheSetResponse setResponse = target.set(cacheName, key, value);
    assertSetResponse(value, setResponse);

    // Successful Get with Hit
    CacheGetResponse getResponse = target.get(cacheName, key);
    assertEquals(CacheGetStatus.HIT, getResponse.status());
    assertEquals(value, getResponse.string().get());
  }

  private void runTtlTest(SimpleCacheClient target) throws Exception {
    String key = UUID.randomUUID().toString();

    // Set Key sync
    CacheSetResponse setRsp = target.set(cacheName, key, "", 1);

    Thread.sleep(1500);

    // Get Key that was just set
    CacheGetResponse rsp = target.get(cacheName, key);
    assertEquals(CacheGetStatus.MISS, rsp.status());
    assertFalse(rsp.string().isPresent());
  }

  private void runMissTest(SimpleCacheClient target) {
    // Get Key that was just set
    CacheGetResponse rsp = target.get(cacheName, UUID.randomUUID().toString());

    assertEquals(CacheGetStatus.MISS, rsp.status());
    assertFalse(rsp.inputStream().isPresent());
    assertFalse(rsp.byteArray().isPresent());
    assertFalse(rsp.byteBuffer().isPresent());
    assertFalse(rsp.string().isPresent());
    assertFalse(rsp.string(Charset.defaultCharset()).isPresent());
  }
}
