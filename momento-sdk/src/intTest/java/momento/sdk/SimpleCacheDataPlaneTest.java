package momento.sdk;

import static momento.sdk.TestUtils.randomString;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import momento.sdk.exceptions.AuthenticationException;
import momento.sdk.exceptions.NotFoundException;
import momento.sdk.exceptions.TimeoutException;
import momento.sdk.messages.CacheDeleteResponse;
import momento.sdk.messages.CacheGetResponse;
import momento.sdk.messages.CacheSetResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests with Async APIs. */
final class SimpleCacheDataPlaneTest extends BaseTestClass {

  private static final Duration DEFAULT_ITEM_TTL_SECONDS = Duration.ofSeconds(60);
  private String authToken;
  private String cacheName;

  @BeforeEach
  void setup() {
    authToken = System.getenv("TEST_AUTH_TOKEN");
    cacheName = System.getenv("TEST_CACHE_NAME");
  }

  @Test
  void getReturnsHitAfterSet() throws Exception {
    runSetAndGetWithHitTest(SimpleCacheClient.builder(authToken, DEFAULT_ITEM_TTL_SECONDS).build());
  }

  @Test
  void cacheMissSuccess() throws Exception {
    runMissTest(SimpleCacheClient.builder(authToken, DEFAULT_ITEM_TTL_SECONDS).build());
  }

  @Test
  void itemDroppedAfterTtlExpires() throws Exception {
    runTtlTest(SimpleCacheClient.builder(authToken, DEFAULT_ITEM_TTL_SECONDS).build());
  }

  @Test
  void badTokenReturnsAuthenticationError() {
    final String badToken =
        "eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJpbnRlZ3JhdGlvbiIsImNwIjoiY29udHJvbC5jZWxsLWFscGhhLWRldi5"
            + "wcmVwcm9kLmEubW9tZW50b2hxLmNvbSIsImMiOiJjYWNoZS5jZWxsLWFscGhhLWRldi5wcmVwcm9kLmEub"
            + "W9tZW50b2hxLmNvbSJ9.gdghdjjfjyehhdkkkskskmmls76573jnajhjjjhjdhnndy";
    try (final SimpleCacheClient client =
        SimpleCacheClient.builder(badToken, DEFAULT_ITEM_TTL_SECONDS).build()) {

      final CacheGetResponse response = client.get(cacheName, "").join();
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

      final CacheGetResponse getResponse = client.get(cacheName, "").join();
      assertThat(getResponse).isInstanceOf(CacheGetResponse.Error.class);
      assertThat(((CacheGetResponse.Error) getResponse))
          .hasCauseInstanceOf(NotFoundException.class);

      final CacheSetResponse setResponse =
          client.set(cacheName, "", "", Duration.ofSeconds(10)).join();
      assertThat(setResponse).isInstanceOf(CacheSetResponse.Error.class);
      assertThat(((CacheSetResponse.Error) setResponse))
          .hasCauseInstanceOf(NotFoundException.class);
    }
  }

  @Test
  public void getWithShortTimeoutReturnsError() {
    try (final SimpleCacheClient client =
        SimpleCacheClient.builder(authToken, DEFAULT_ITEM_TTL_SECONDS)
            .requestTimeout(Duration.ofMillis(1))
            .build()) {

      final CacheGetResponse response = client.get("cache", "key").join();
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
      client.set(cacheName, emptyKey, emptyValue).get();
      final CacheGetResponse response = client.get(cacheName, emptyKey).get();
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

      client.set(cacheName, key, value).get();
      final CacheGetResponse getResponse = client.get(cacheName, key).get();
      assertThat(getResponse).isInstanceOf(CacheGetResponse.Hit.class);
      assertThat(((CacheGetResponse.Hit) getResponse).valueString()).isEqualTo(value);

      final CacheDeleteResponse deleteResponse = client.delete(cacheName, key).get();
      assertThat(deleteResponse).isInstanceOf(CacheDeleteResponse.Success.class);

      final CacheGetResponse getAfterDeleteResponse = client.get(cacheName, key).get();
      assertThat(getAfterDeleteResponse).isInstanceOf(CacheGetResponse.Miss.class);
    }
  }

  @Test
  public void setWithShortTimeoutReturnsError() {
    try (SimpleCacheClient client =
        SimpleCacheClient.builder(authToken, DEFAULT_ITEM_TTL_SECONDS)
            .requestTimeout(Duration.ofMillis(1))
            .build()) {

      final CacheSetResponse response = client.set("cache", "key", "value").join();
      assertThat(response).isInstanceOf(CacheSetResponse.Error.class);
      assertThat(((CacheSetResponse.Error) response)).hasCauseInstanceOf(TimeoutException.class);
    }
  }

  private void runSetAndGetWithHitTest(SimpleCacheClient target) throws Exception {
    final String key = randomString("key");
    final String value = randomString("value");

    // Successful Set
    final CacheSetResponse setResponse = target.set(cacheName, key, value).join();
    assertThat(setResponse).isInstanceOf(CacheSetResponse.Success.class);
    assertThat(((CacheSetResponse.Success) setResponse).valueString()).isEqualTo(value);
    assertThat(((CacheSetResponse.Success) setResponse).valueByteArray())
        .isEqualTo(value.getBytes());

    // Successful Get with Hit
    final CacheGetResponse getResponse = target.get(cacheName, key).get();
    assertThat(getResponse).isInstanceOf(CacheGetResponse.Hit.class);
    assertThat(((CacheGetResponse.Hit) getResponse).valueString()).isEqualTo(value);
  }

  private void runTtlTest(SimpleCacheClient target) throws Exception {
    final String key = randomString("key");

    // Set Key sync
    target.set(cacheName, key, "", Duration.ofSeconds(1));

    Thread.sleep(2000);

    // Get Key that was just set
    final CacheGetResponse rsp = target.get(cacheName, key).get();
    assertThat(rsp).isInstanceOf(CacheGetResponse.Miss.class);
  }

  private void runMissTest(SimpleCacheClient target) throws Exception {
    // Get key that was not set
    final CacheGetResponse response = target.get(cacheName, randomString("key")).get();
    assertThat(response).isInstanceOf(CacheGetResponse.Miss.class);
  }
}
