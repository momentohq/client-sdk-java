package momento.sdk;

import static momento.sdk.OtelTestHelpers.setOtelSDK;
import static momento.sdk.OtelTestHelpers.startIntegrationTestOtel;
import static momento.sdk.OtelTestHelpers.stopIntegrationTestOtel;
import static momento.sdk.OtelTestHelpers.verifyGetTrace;
import static momento.sdk.OtelTestHelpers.verifySetTrace;
import static momento.sdk.ScsDataTestHelper.assertSetResponse;
import static momento.sdk.TestUtils.randomString;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.opentelemetry.sdk.OpenTelemetrySdk;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import momento.sdk.exceptions.AuthenticationException;
import momento.sdk.exceptions.NotFoundException;
import momento.sdk.exceptions.TimeoutException;
import momento.sdk.messages.CacheGetResponse;
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
  void badTokenReturnsAuthenticationError() {
    final String badToken =
        "eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJpbnRlZ3JhdGlvbiIsImNwIjoiY29udHJvbC5jZWxsLWFscGhhLWRldi5"
            + "wcmVwcm9kLmEubW9tZW50b2hxLmNvbSIsImMiOiJjYWNoZS5jZWxsLWFscGhhLWRldi5wcmVwcm9kLmEub"
            + "W9tZW50b2hxLmNvbSJ9.gdghdjjfjyehhdkkkskskmmls76573jnajhjjjhjdhnndy";
    try (final SimpleCacheClient client =
        SimpleCacheClient.builder(badToken, DEFAULT_ITEM_TTL_SECONDS).build()) {

      final CacheGetResponse response = client.getAsync(cacheName, "").join();
      assertThat(response).isInstanceOf(CacheGetResponse.Error.class);
      assertThat(((CacheGetResponse.Error) response))
          .hasCauseInstanceOf(AuthenticationException.class);
    }
  }

  @Test
  public void nonExistentCacheNameReturnsErrorOnGetOrSet() {
    try (final SimpleCacheClient client =
        SimpleCacheClient.builder(authToken, DEFAULT_ITEM_TTL_SECONDS).build()) {
      final String cacheName = randomString("name");

      final CacheGetResponse response = client.getAsync(cacheName, "").join();
      assertThat(response).isInstanceOf(CacheGetResponse.Error.class);
      assertThat(((CacheGetResponse.Error) response)).hasCauseInstanceOf(NotFoundException.class);

      ExecutionException getException =
          assertThrows(
              ExecutionException.class, () -> client.setAsync(cacheName, "", "", 10).get());
      assertTrue(getException.getCause() instanceof NotFoundException);
    }
  }

  @Test
  public void getWithShortTimeoutReturnsError() {
    try (final SimpleCacheClient client =
        SimpleCacheClient.builder(authToken, DEFAULT_ITEM_TTL_SECONDS)
            .requestTimeout(Duration.ofMillis(1))
            .build()) {

      final CacheGetResponse response = client.getAsync("cache", "key").join();
      assertThat(response).isInstanceOf(CacheGetResponse.Error.class);
      assertThat(((CacheGetResponse.Error) response)).hasCauseInstanceOf(TimeoutException.class);
    }
  }

  @Test
  public void allowEmptyKeyValues() throws Exception {
    try (final SimpleCacheClient client =
        SimpleCacheClient.builder(authToken, DEFAULT_ITEM_TTL_SECONDS).build()) {
      final String emptyKey = "";
      final String emptyValue = "";
      client.setAsync(cacheName, emptyKey, emptyValue).get();
      final CacheGetResponse response = client.getAsync(cacheName, emptyKey).get();
      assertThat(response).isInstanceOf(CacheGetResponse.Hit.class);
      assertThat(((CacheGetResponse.Hit) response).valueString()).isEqualTo(emptyValue);
    }
  }

  @Test
  public void deleteAsyncHappyPath() throws Exception {
    try (final SimpleCacheClient client =
        SimpleCacheClient.builder(authToken, DEFAULT_ITEM_TTL_SECONDS).build()) {
      final String key = "key";
      final String value = "value";
      client.setAsync(cacheName, key, value).get();
      final CacheGetResponse getResponse = client.getAsync(cacheName, key).get();
      assertThat(getResponse).isInstanceOf(CacheGetResponse.Hit.class);
      assertThat(((CacheGetResponse.Hit) getResponse).valueString()).isEqualTo(value);
      client.deleteAsync(cacheName, key).get();
      final CacheGetResponse getAfterDeleteResponse = client.getAsync(cacheName, key).get();
      assertThat(getAfterDeleteResponse).isInstanceOf(CacheGetResponse.Miss.class);
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
    final String key = randomString("key");
    final String value = randomString("value");

    // Successful Set
    final CompletableFuture<CacheSetResponse> setResponse = target.setAsync(cacheName, key, value);
    assertSetResponse(value, setResponse.get());

    // Successful Get with Hit
    final CacheGetResponse getResponse = target.getAsync(cacheName, key).get();
    assertThat(getResponse).isInstanceOf(CacheGetResponse.Hit.class);
    assertThat(((CacheGetResponse.Hit) getResponse).valueString()).isEqualTo(value);
  }

  private void runTtlTest(SimpleCacheClient target) throws Exception {
    final String key = randomString("key");

    // Set Key sync
    target.setAsync(cacheName, key, "", 1);

    Thread.sleep(2000);

    // Get Key that was just set
    final CacheGetResponse rsp = target.getAsync(cacheName, key).get();
    assertThat(rsp).isInstanceOf(CacheGetResponse.Miss.class);
  }

  private void runMissTest(SimpleCacheClient target) throws Exception {
    // Get key that was not set
    final CacheGetResponse response = target.getAsync(cacheName, randomString("key")).get();
    assertThat(response).isInstanceOf(CacheGetResponse.Miss.class);
  }
}
