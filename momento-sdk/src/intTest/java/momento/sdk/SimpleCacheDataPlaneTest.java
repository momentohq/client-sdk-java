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

  private static final int DEFAULT_ITEM_TTL_SECONDS = 60;
  private String authToken;
  private String cacheName;

  @BeforeEach
  void setup() {
    authToken = System.getenv("TEST_AUTH_TOKEN");
    cacheName = System.getenv("TEST_CACHE_NAME");
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

      final CacheSetResponse setResponse = client.set(cacheName, "", "", 10).join();
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
}
