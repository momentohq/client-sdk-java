package momento.sdk;

import static momento.sdk.OtelTestHelpers.setOtelSDK;
import static momento.sdk.OtelTestHelpers.startIntegrationTestOtel;
import static momento.sdk.OtelTestHelpers.stopIntegrationTestOtel;
import static momento.sdk.OtelTestHelpers.verifyGetTrace;
import static momento.sdk.OtelTestHelpers.verifySetTrace;
import static momento.sdk.ScsDataTestHelper.assertSetResponse;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.opentelemetry.sdk.OpenTelemetrySdk;
import java.io.IOException;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import momento.sdk.exceptions.AuthenticationException;
import momento.sdk.exceptions.NotFoundException;
import momento.sdk.exceptions.TimeoutException;
import momento.sdk.messages.CacheGetResponse;
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
  public void badTokenReturnsAuthenticationError() {
    final String badToken =
        "eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJpbnRlZ3JhdGlvbiIsImNwIjoiY29udHJvbC5jZWxsLWFscGhhLWRldi5"
            + "wcmVwcm9kLmEubW9tZW50b2hxLmNvbSIsImMiOiJjYWNoZS5jZWxsLWFscGhhLWRldi5wcmVwcm9kLmEub"
            + "W9tZW50b2hxLmNvbSJ9.gdghdjjfjyehhdkkkskskmmls76573jnajhjjjhjdhnndy";
    try (final SimpleCacheClient client =
        SimpleCacheClient.builder(badToken, DEFAULT_ITEM_TTL_SECONDS).build()) {

      final CacheGetResponse response = client.get(cacheName, "");
      assertThat(response).isInstanceOf(CacheGetResponse.Error.class);
      assertThat(((CacheGetResponse.Error) response))
          .hasCauseInstanceOf(AuthenticationException.class);
    }
  }

  @Test
  public void nonExistentCacheNameReturnsErrorOnGetOrSet() {
    try (final SimpleCacheClient client =
        SimpleCacheClient.builder(authToken, DEFAULT_ITEM_TTL_SECONDS).build()) {
      final String cacheName = UUID.randomUUID().toString();

      final CacheGetResponse response = client.get(cacheName, "");
      assertThat(response).isInstanceOf(CacheGetResponse.Error.class);
      assertThat(((CacheGetResponse.Error) response)).hasCauseInstanceOf(NotFoundException.class);

      assertThrows(NotFoundException.class, () -> client.set(cacheName, "", "", 10));
    }
  }

  @Test
  public void setGetDeleteWithByteKeyValuesMustSucceed() {
    final byte[] key = {0x01, 0x02, 0x03, 0x04};
    final byte[] value = {0x05, 0x06, 0x07, 0x08};
    try (final SimpleCacheClient cache =
        SimpleCacheClient.builder(authToken, DEFAULT_ITEM_TTL_SECONDS).build()) {
      cache.set(cacheName, key, value, 60);

      final CacheGetResponse getResponse = cache.get(cacheName, key);
      assertThat(getResponse).isInstanceOf(CacheGetResponse.Hit.class);
      assertThat(((CacheGetResponse.Hit) getResponse).valueByteArray()).isEqualTo(value);

      cache.delete(cacheName, key);
      final CacheGetResponse getAfterDeleteResponse = cache.get(cacheName, key);
      assertThat(getAfterDeleteResponse).isInstanceOf(CacheGetResponse.Miss.class);
    }
  }

  @Test
  public void getWithShortTimeoutReturnsError() {
    try (final SimpleCacheClient client =
        SimpleCacheClient.builder(authToken, DEFAULT_ITEM_TTL_SECONDS)
            .requestTimeout(Duration.ofMillis(1))
            .build()) {
      final CacheGetResponse response = client.get("cache", "key");
      assertThat(response).isInstanceOf(CacheGetResponse.Error.class);
      assertThat(((CacheGetResponse.Error) response)).hasCauseInstanceOf(TimeoutException.class);
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

  @Test
  public void allowEmptyKeyValues() {
    try (final SimpleCacheClient client =
        SimpleCacheClient.builder(authToken, DEFAULT_ITEM_TTL_SECONDS).build()) {
      final String emptyKey = "";
      final String emptyValue = "";
      client.set(cacheName, emptyKey, emptyValue);
      final CacheGetResponse response = client.get(cacheName, emptyKey);
      assertThat(response).isInstanceOf(CacheGetResponse.Hit.class);
      assertThat(((CacheGetResponse.Hit) response).valueString()).isEqualTo(emptyValue);
    }
  }

  private void runSetAndGetWithHitTest(SimpleCacheClient target) throws IOException {
    final String key = UUID.randomUUID().toString();
    final String value = UUID.randomUUID().toString();

    // Successful Set
    final CacheSetResponse setResponse = target.set(cacheName, key, value);
    assertSetResponse(value, setResponse);

    // Successful Get with Hit
    final CacheGetResponse getResponse = target.get(cacheName, key);
    assertThat(getResponse).isInstanceOf(CacheGetResponse.Hit.class);
    assertThat(((CacheGetResponse.Hit) getResponse).valueString()).isEqualTo(value);
  }

  private void runTtlTest(SimpleCacheClient target) throws Exception {
    final String key = UUID.randomUUID().toString();

    // Set Key sync
    target.set(cacheName, key, "", 1);

    Thread.sleep(2000);

    // Get Key that was just set
    final CacheGetResponse rsp = target.get(cacheName, key);
    assertThat(rsp).isInstanceOf(CacheGetResponse.Miss.class);
  }

  private void runMissTest(SimpleCacheClient target) {
    // Get Key that was not set
    final CacheGetResponse rsp = target.get(cacheName, UUID.randomUUID().toString());
    assertThat(rsp).isInstanceOf(CacheGetResponse.Miss.class);
  }
}
