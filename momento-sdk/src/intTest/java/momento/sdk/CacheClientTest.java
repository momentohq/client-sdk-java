package momento.sdk;

import static momento.sdk.TestUtils.randomString;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.time.Duration;
import momento.sdk.auth.CredentialProvider;
import momento.sdk.auth.StringCredentialProvider;
import momento.sdk.config.Configurations;
import momento.sdk.exceptions.AuthenticationException;
import momento.sdk.exceptions.InvalidArgumentException;
import momento.sdk.exceptions.NotFoundException;
import momento.sdk.exceptions.ServerUnavailableException;
import momento.sdk.responses.CacheDeleteResponse;
import momento.sdk.responses.CacheGetResponse;
import momento.sdk.responses.CacheIncrementResponse;
import momento.sdk.responses.CacheSetIfNotExistsResponse;
import momento.sdk.responses.CacheSetResponse;
import momento.sdk.responses.CreateCacheResponse;
import momento.sdk.responses.FlushCacheResponse;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Just includes a happy test path that interacts with both control and data plane clients. */
final class CacheClientTest extends BaseTestClass {

  private static final Duration DEFAULT_TTL_SECONDS = Duration.ofSeconds(60);

  private CacheClient target;

  private String cacheName;

  private static final String JWT_HEADER_BASE64 = "eyJhbGciOiJIUzUxMiJ9";
  private static final String JWT_INVALID_SIGNATURE_BASE64 =
      "gdghdjjfjyehhdkkkskskmmls76573jnajhjjjhjdhnndy";

  // {"sub":"squirrel","cp":"invalidcontrol.cell-alpha-dev.preprod.a.momentohq.com","c":"cache.cell-alpha-dev.preprod.a.momentohq.com"}
  private static final String JWT_PAYLOAD_BAD_CONTROL_PLANE_BASE64 =
      "eyJzdWIiOiJzcXVpcnJlbCIsImNwIjoiaW52YWxpZGNvbnRyb2wuY2VsbC1hbHBoYS1kZXYucHJlcHJvZC5hLm1vb"
          + "WVudG9ocS5jb20iLCJjIjoiY2FjaGUuY2VsbC1hbHBoYS1kZXYucHJlcHJvZC5hLm1vbWVudG9ocS5jb20ifQ";

  // {"sub":"squirrel","cp":"control.cell-alpha-dev.preprod.a.momentohq.com","c":"invalidcache.cell-alpha-dev.preprod.a.momentohq.com"}
  private static final String JWT_PAYLOAD_BAD_DATA_PLANE_BASE64 =
      "eyJzdWIiOiJzcXVpcnJlbCIsImNwIjoiY29udHJvbC5jZWxsLWFscGhhLWRldi5wcmVwcm9kLmEubW9tZW50b2hxL"
          + "mNvbSIsImMiOiJpbnZhbGlkY2FjaGUuY2VsbC1hbHBoYS1kZXYucHJlcHJvZC5hLm1vbWVudG9ocS5jb20ifQ";

  // These JWTs will result in UNAUTHENTICATED from the reachable backend since they have made up
  // signatures
  private static final String BAD_CONTROL_PLANE_JWT =
      JWT_HEADER_BASE64
          + "."
          + JWT_PAYLOAD_BAD_CONTROL_PLANE_BASE64
          + "."
          + JWT_INVALID_SIGNATURE_BASE64;
  private static final CredentialProvider BAD_CONTROL_PLANE_PROVIDER =
      new StringCredentialProvider(BAD_CONTROL_PLANE_JWT);
  private static final String BAD_DATA_PLANE_JWT =
      JWT_HEADER_BASE64
          + "."
          + JWT_PAYLOAD_BAD_DATA_PLANE_BASE64
          + "."
          + JWT_INVALID_SIGNATURE_BASE64;
  private static final CredentialProvider BAD_DATA_PLANE_PROVIDER =
      new StringCredentialProvider(BAD_DATA_PLANE_JWT);

  @BeforeEach
  void setup() {
    target =
        CacheClient.builder(credentialProvider, Configurations.Laptop.latest(), DEFAULT_TTL_SECONDS)
            .build();
    cacheName = System.getenv("TEST_CACHE_NAME");
    target.createCache(cacheName).join();
  }

  @AfterEach
  void teardown() {
    target.deleteCache(cacheName).join();
    target.close();
  }

  @Test
  public void createCacheGetSetDeleteValuesAndDeleteCache() {
    final String alternateCacheName = randomString("alternateName");
    final String key = randomString("key");
    final String value = randomString("value");

    target.createCache(alternateCacheName).join();
    try {
      target.set(cacheName, key, value).join();

      final CacheGetResponse getResponse = target.get(cacheName, key).join();
      assertThat(getResponse).isInstanceOf(CacheGetResponse.Hit.class);
      assertThat(((CacheGetResponse.Hit) getResponse).valueString()).isEqualTo(value);

      final CacheDeleteResponse deleteResponse = target.delete(cacheName, key).join();
      assertThat(deleteResponse).isInstanceOf(CacheDeleteResponse.Success.class);

      final CacheGetResponse getAfterDeleteResponse = target.get(cacheName, key).join();
      assertThat(getAfterDeleteResponse).isInstanceOf(CacheGetResponse.Miss.class);

      final CacheGetResponse getForKeyInSomeOtherCache = target.get(alternateCacheName, key).join();
      assertThat(getForKeyInSomeOtherCache).isInstanceOf(CacheGetResponse.Miss.class);
    } finally {
      target.deleteCache(alternateCacheName).join();
    }
  }

  @Test
  public void shouldFlushCacheContents() {
    final String key = randomString("key");
    final String value = randomString("value");
    final Duration ttl1Hour = Duration.ofHours(1);

    try {
      assertThat(target.set(cacheName, key, value, ttl1Hour))
          .succeedsWithin(FIVE_SECONDS)
          .asInstanceOf(InstanceOfAssertFactories.type(CacheSetResponse.Success.class))
          .satisfies(success -> assertThat(success.value()).isEqualTo(value));

      // Execute Flush
      assertThat(target.flushCache(cacheName))
          .succeedsWithin(FIVE_SECONDS)
          .isInstanceOf(FlushCacheResponse.Success.class);

      // Verify that previously set key is now a MISS
      assertThat(target.get(cacheName, key))
          .succeedsWithin(FIVE_SECONDS)
          .isInstanceOf(CacheGetResponse.Miss.class);
    } finally {
      target.deleteCache(cacheName).join();
    }
  }

  @Test
  public void shouldReturnNotFoundWhenCacheToFlushDoesNotExist() {
    assertThat(target.flushCache(randomString("name")))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(FlushCacheResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(NotFoundException.class));
  }

  @Test
  public void shouldReturnIllegalArgWhenCacheNameToFlushIsInvalid() {
    assertThat(target.flushCache(null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(FlushCacheResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void throwsExceptionWhenClientUsesNegativeDefaultTtl() {
    //noinspection resource
    assertThatExceptionOfType(InvalidArgumentException.class)
        .isThrownBy(
            () ->
                CacheClient.builder(
                        credentialProvider, Configurations.Laptop.latest(), Duration.ofDays(-1))
                    .build());
  }

  @Test
  public void initializesSdkAndCanHitDataPlaneForUnreachableControlPlane() {
    try (final CacheClient client =
        CacheClient.builder(
                BAD_CONTROL_PLANE_PROVIDER, Configurations.Laptop.latest(), DEFAULT_TTL_SECONDS)
            .build()) {
      // Unable to hit control plane
      assertThat(client.createCache(randomString("cacheName")))
          .succeedsWithin(FIVE_SECONDS)
          .asInstanceOf(InstanceOfAssertFactories.type(CreateCacheResponse.Error.class))
          .satisfies(
              error -> assertThat(error).hasCauseInstanceOf(ServerUnavailableException.class));

      // But gets a valid response from Data plane
      assertThat(client.get("helloCache", "key"))
          .succeedsWithin(FIVE_SECONDS)
          .asInstanceOf(InstanceOfAssertFactories.type(CacheGetResponse.Error.class))
          .satisfies(
              error -> {
                assertThat(error).hasCauseInstanceOf(AuthenticationException.class);
                assertThat(error.getTransportErrorDetails()).isNotEmpty();
              });

      assertThat(client.set("helloCache", "key", "value"))
          .succeedsWithin(FIVE_SECONDS)
          .asInstanceOf(InstanceOfAssertFactories.type(CacheSetResponse.Error.class))
          .satisfies(error -> assertThat(error).hasCauseInstanceOf(AuthenticationException.class));
    }
  }

  @Test
  public void initializesSdkAndCanHitControlPlaneForUnreachableDataPlane() {
    try (final CacheClient client =
        CacheClient.builder(
                BAD_DATA_PLANE_PROVIDER, Configurations.Laptop.latest(), DEFAULT_TTL_SECONDS)
            .build()) {

      // Can reach control plane.
      assertThat(client.createCache(randomString("cacheName")))
          .succeedsWithin(FIVE_SECONDS)
          .asInstanceOf(InstanceOfAssertFactories.type(CreateCacheResponse.Error.class))
          .satisfies(error -> assertThat(error).hasCauseInstanceOf(AuthenticationException.class));

      // Unable to reach data plane
      assertThat(client.set("helloCache", "key", "value"))
          .succeedsWithin(FIVE_SECONDS)
          .asInstanceOf(InstanceOfAssertFactories.type(CacheSetResponse.Error.class))
          .satisfies(
              error -> assertThat(error).hasCauseInstanceOf(ServerUnavailableException.class));

      assertThat(client.get("helloCache", "key"))
          .succeedsWithin(FIVE_SECONDS)
          .asInstanceOf(InstanceOfAssertFactories.type(CacheGetResponse.Error.class))
          .satisfies(
              error -> assertThat(error).hasCauseInstanceOf(ServerUnavailableException.class));
    }
  }

  @Test
  public void shouldReturnCacheIncrementedValuesWithStringField() {
    final String field = "field";

    CacheIncrementResponse incrementResponse =
        target.increment(cacheName, field, 1, DEFAULT_TTL_SECONDS).join();

    assertThat(incrementResponse).isInstanceOf(CacheIncrementResponse.Success.class);
    assertThat(((CacheIncrementResponse.Success) incrementResponse).valueNumber()).isEqualTo(1);

    // increment with ttl specified
    incrementResponse = target.increment(cacheName, field, 50, DEFAULT_TTL_SECONDS).join();

    assertThat(incrementResponse).isInstanceOf(CacheIncrementResponse.Success.class);
    assertThat(((CacheIncrementResponse.Success) incrementResponse).valueNumber()).isEqualTo(51);

    // increment without ttl specified
    incrementResponse = target.increment(cacheName, field, -1051).join();

    assertThat(incrementResponse).isInstanceOf(CacheIncrementResponse.Success.class);
    assertThat(((CacheIncrementResponse.Success) incrementResponse).valueNumber()).isEqualTo(-1000);

    CacheGetResponse getResp = target.get(cacheName, field).join();
    assertThat(getResp).isInstanceOf(CacheGetResponse.Hit.class);
    assertThat(((CacheGetResponse.Hit) getResp).valueString()).isEqualTo("-1000");
  }

  @Test
  public void shouldReturnCacheIncrementedValuesWithByteArrayField() {
    final byte[] field = "field".getBytes();

    CacheIncrementResponse incrementResponse =
        target.increment(cacheName, field, 1, DEFAULT_TTL_SECONDS).join();

    assertThat(incrementResponse).isInstanceOf(CacheIncrementResponse.Success.class);
    assertThat(((CacheIncrementResponse.Success) incrementResponse).valueNumber()).isEqualTo(1);

    // increment with ttl specified
    incrementResponse = target.increment(cacheName, field, 50, DEFAULT_TTL_SECONDS).join();

    assertThat(incrementResponse).isInstanceOf(CacheIncrementResponse.Success.class);
    assertThat(((CacheIncrementResponse.Success) incrementResponse).valueNumber()).isEqualTo(51);

    // increment without ttl specified
    incrementResponse = target.increment(cacheName, field, -1051).join();

    assertThat(incrementResponse).isInstanceOf(CacheIncrementResponse.Success.class);
    assertThat(((CacheIncrementResponse.Success) incrementResponse).valueNumber()).isEqualTo(-1000);

    CacheGetResponse getResp = target.get(cacheName, field).join();
    assertThat(getResp).isInstanceOf(CacheGetResponse.Hit.class);
    assertThat(((CacheGetResponse.Hit) getResp).valueString()).isEqualTo("-1000");
  }

  @Test
  public void shouldFailCacheIncrementedValuesWhenNullCacheName() {
    final String field = "field";

    // With ttl specified
    CacheIncrementResponse cacheIncrementResponse =
        target.increment(null, field, 1, DEFAULT_TTL_SECONDS).join();
    assertThat(cacheIncrementResponse).isInstanceOf(CacheIncrementResponse.Error.class);

    // Without ttl specified
    cacheIncrementResponse = target.increment(null, field, 1).join();
    assertThat(cacheIncrementResponse).isInstanceOf(CacheIncrementResponse.Error.class);
  }

  @Test
  public void shouldFailCacheIncrementedValuesWhenCacheNotExist() {
    final String cacheName = "fake-cache";
    final String field = "field";

    // With ttl specified
    CacheIncrementResponse cacheIncrementResponse =
        target.increment(cacheName, field, 1, DEFAULT_TTL_SECONDS).join();
    assertThat(cacheIncrementResponse).isInstanceOf(CacheIncrementResponse.Error.class);

    // Without ttl specified
    cacheIncrementResponse = target.increment(cacheName, field, 1).join();
    assertThat(cacheIncrementResponse).isInstanceOf(CacheIncrementResponse.Error.class);
  }

  @Test
  public void shouldSetStringValueToStringKeyWhenKeyNotExistsWithTtl() {
    final String key = randomString("test-key");
    final String value = randomString("test-value");

    CacheSetIfNotExistsResponse cacheSetIfNotExistsResponse =
        target.setIfNotExists(cacheName, key, value, DEFAULT_TTL_SECONDS).join();

    assertThat(cacheSetIfNotExistsResponse).isInstanceOf(CacheSetIfNotExistsResponse.Stored.class);
    assertThat(((CacheSetIfNotExistsResponse.Stored) cacheSetIfNotExistsResponse).keyString())
        .isEqualTo(key);
    assertThat(((CacheSetIfNotExistsResponse.Stored) cacheSetIfNotExistsResponse).valueString())
        .isEqualTo(value);

    CacheGetResponse cacheGetResponse = target.get(cacheName, key).join();
    assertThat(cacheGetResponse).isInstanceOf(CacheGetResponse.Hit.class);
    assertThat(((CacheGetResponse.Hit) cacheGetResponse).valueString()).isEqualTo(value);
  }

  @Test
  public void shouldSetStringValueToStringKeyWhenKeyNotExistsWithoutTtl() {
    final String key = randomString("test-key");
    final String value = randomString("test-value");

    CacheSetIfNotExistsResponse cacheSetIfNotExistsResponse =
        target.setIfNotExists(cacheName, key, value).join();

    assertThat(cacheSetIfNotExistsResponse).isInstanceOf(CacheSetIfNotExistsResponse.Stored.class);
    assertThat(((CacheSetIfNotExistsResponse.Stored) cacheSetIfNotExistsResponse).keyString())
        .isEqualTo(key);
    assertThat(((CacheSetIfNotExistsResponse.Stored) cacheSetIfNotExistsResponse).valueString())
        .isEqualTo(value);

    CacheGetResponse cacheGetResponse = target.get(cacheName, key).join();
    assertThat(cacheGetResponse).isInstanceOf(CacheGetResponse.Hit.class);
    assertThat(((CacheGetResponse.Hit) cacheGetResponse).valueString()).isEqualTo(value);
  }

  @Test
  public void shouldSetByteArrayValueToStringKeyWhenKeyNotExistsWithTtl() {
    final String key = randomString("test-key");
    final byte[] value = "test-value".getBytes();

    CacheSetIfNotExistsResponse cacheSetIfNotExistsResponse =
        target.setIfNotExists(cacheName, key, value, DEFAULT_TTL_SECONDS).join();

    assertThat(cacheSetIfNotExistsResponse).isInstanceOf(CacheSetIfNotExistsResponse.Stored.class);
    assertThat(((CacheSetIfNotExistsResponse.Stored) cacheSetIfNotExistsResponse).keyString())
        .isEqualTo(key);
    assertThat(((CacheSetIfNotExistsResponse.Stored) cacheSetIfNotExistsResponse).valueByteArray())
        .isEqualTo(value);

    CacheGetResponse cacheGetResponse = target.get(cacheName, key).join();
    assertThat(cacheGetResponse).isInstanceOf(CacheGetResponse.Hit.class);
    assertThat(((CacheGetResponse.Hit) cacheGetResponse).valueByteArray()).isEqualTo(value);
  }

  @Test
  public void shouldSetByteArrayValueToStringKeyWhenKeyNotExistsWithoutTtl() {
    final String key = randomString("test-key");
    final byte[] value = "test-value".getBytes();

    CacheSetIfNotExistsResponse cacheSetIfNotExistsResponse =
        target.setIfNotExists(cacheName, key, value).join();

    assertThat(cacheSetIfNotExistsResponse).isInstanceOf(CacheSetIfNotExistsResponse.Stored.class);
    assertThat(((CacheSetIfNotExistsResponse.Stored) cacheSetIfNotExistsResponse).keyString())
        .isEqualTo(key);
    assertThat(((CacheSetIfNotExistsResponse.Stored) cacheSetIfNotExistsResponse).valueByteArray())
        .isEqualTo(value);

    CacheGetResponse cacheGetResponse = target.get(cacheName, key).join();
    assertThat(cacheGetResponse).isInstanceOf(CacheGetResponse.Hit.class);
    assertThat(((CacheGetResponse.Hit) cacheGetResponse).valueByteArray()).isEqualTo(value);
  }

  @Test
  public void shouldSetStringValueToByteArrayKeyWhenKeyNotExistsWithTtl() {
    final byte[] key = "test-key".getBytes();
    final String value = "test-value";

    CacheSetIfNotExistsResponse cacheSetIfNotExistsResponse =
        target.setIfNotExists(cacheName, key, value, DEFAULT_TTL_SECONDS).join();

    assertThat(cacheSetIfNotExistsResponse).isInstanceOf(CacheSetIfNotExistsResponse.Stored.class);
    assertThat(((CacheSetIfNotExistsResponse.Stored) cacheSetIfNotExistsResponse).keyByteArray())
        .isEqualTo(key);
    assertThat(((CacheSetIfNotExistsResponse.Stored) cacheSetIfNotExistsResponse).valueString())
        .isEqualTo(value);

    CacheGetResponse cacheGetResponse = target.get(cacheName, key).join();
    assertThat(cacheGetResponse).isInstanceOf(CacheGetResponse.Hit.class);
    assertThat(((CacheGetResponse.Hit) cacheGetResponse).valueString()).isEqualTo(value);
  }

  @Test
  public void shouldSetStringValueToByteArrayKeyWhenKeyNotExistsWithoutTtl() {
    final byte[] key = "test-key".getBytes();
    final String value = "test-value";

    CacheSetIfNotExistsResponse cacheSetIfNotExistsResponse =
        target.setIfNotExists(cacheName, key, value).join();

    assertThat(cacheSetIfNotExistsResponse).isInstanceOf(CacheSetIfNotExistsResponse.Stored.class);
    assertThat(((CacheSetIfNotExistsResponse.Stored) cacheSetIfNotExistsResponse).keyByteArray())
        .isEqualTo(key);
    assertThat(((CacheSetIfNotExistsResponse.Stored) cacheSetIfNotExistsResponse).valueString())
        .isEqualTo(value);

    CacheGetResponse cacheGetResponse = target.get(cacheName, key).join();
    assertThat(cacheGetResponse).isInstanceOf(CacheGetResponse.Hit.class);
    assertThat(((CacheGetResponse.Hit) cacheGetResponse).valueString()).isEqualTo(value);
  }

  @Test
  public void shouldSetByteArrayValueToByteArrayKeyWhenKeyNotExistsWithttl() {
    final byte[] key = "test-key".getBytes();
    final byte[] value = "test-value".getBytes();

    CacheSetIfNotExistsResponse cacheSetIfNotExistsResponse =
        target.setIfNotExists(cacheName, key, value, DEFAULT_TTL_SECONDS).join();

    assertThat(cacheSetIfNotExistsResponse).isInstanceOf(CacheSetIfNotExistsResponse.Stored.class);
    assertThat(((CacheSetIfNotExistsResponse.Stored) cacheSetIfNotExistsResponse).keyByteArray())
        .isEqualTo(key);
    assertThat(((CacheSetIfNotExistsResponse.Stored) cacheSetIfNotExistsResponse).valueByteArray())
        .isEqualTo(value);

    CacheGetResponse cacheGetResponse = target.get(cacheName, key).join();
    assertThat(cacheGetResponse).isInstanceOf(CacheGetResponse.Hit.class);
    assertThat(((CacheGetResponse.Hit) cacheGetResponse).valueByteArray()).isEqualTo(value);
  }

  @Test
  public void shouldSetByteArrayValueToByteArrayKeyWhenKeyNotExistsWithoutTtl() {
    final byte[] key = "test-key".getBytes();
    final byte[] value = "test-value".getBytes();

    CacheSetIfNotExistsResponse cacheSetIfNotExistsResponse =
        target.setIfNotExists(cacheName, key, value).join();

    assertThat(cacheSetIfNotExistsResponse).isInstanceOf(CacheSetIfNotExistsResponse.Stored.class);
    assertThat(((CacheSetIfNotExistsResponse.Stored) cacheSetIfNotExistsResponse).keyByteArray())
        .isEqualTo(key);
    assertThat(((CacheSetIfNotExistsResponse.Stored) cacheSetIfNotExistsResponse).valueByteArray())
        .isEqualTo(value);

    CacheGetResponse cacheGetResponse = target.get(cacheName, key).join();
    assertThat(cacheGetResponse).isInstanceOf(CacheGetResponse.Hit.class);
    assertThat(((CacheGetResponse.Hit) cacheGetResponse).valueByteArray()).isEqualTo(value);
  }

  @Test
  public void shouldNotSetValueToKeyWhenKeyExists() {
    final String key = "test-key";
    final String oldValue = "old-test-value";
    final String newValue = "new-test-value";

    CacheSetIfNotExistsResponse cacheSetIfNotExistsResponse =
        target.setIfNotExists(cacheName, key, oldValue, DEFAULT_TTL_SECONDS).join();

    assertThat(cacheSetIfNotExistsResponse).isInstanceOf(CacheSetIfNotExistsResponse.Stored.class);
    assertThat(((CacheSetIfNotExistsResponse.Stored) cacheSetIfNotExistsResponse).keyString())
        .isEqualTo(key);
    assertThat(((CacheSetIfNotExistsResponse.Stored) cacheSetIfNotExistsResponse).valueString())
        .isEqualTo(oldValue);

    // When ttl is specified
    cacheSetIfNotExistsResponse =
        target.setIfNotExists(cacheName, key, newValue, DEFAULT_TTL_SECONDS).join();

    assertThat(cacheSetIfNotExistsResponse)
        .isInstanceOf(CacheSetIfNotExistsResponse.NotStored.class);

    CacheGetResponse cacheGetResponse = target.get(cacheName, key).join();
    assertThat(cacheGetResponse).isInstanceOf(CacheGetResponse.Hit.class);
    assertThat(((CacheGetResponse.Hit) cacheGetResponse).valueString()).isEqualTo(oldValue);

    // When ttl is not specified
    cacheSetIfNotExistsResponse = target.setIfNotExists(cacheName, key, newValue).join();

    assertThat(cacheSetIfNotExistsResponse)
        .isInstanceOf(CacheSetIfNotExistsResponse.NotStored.class);

    cacheGetResponse = target.get(cacheName, key).join();
    assertThat(cacheGetResponse).isInstanceOf(CacheGetResponse.Hit.class);
    assertThat(((CacheGetResponse.Hit) cacheGetResponse).valueString()).isEqualTo(oldValue);
  }

  @Test
  public void shouldFailSetValueToKeyWhenCacheNotExist() {
    final String cacheName = "fake-cache";
    final String key = "test-key";
    final String value = "old-test-value";

    // With ttl specified
    CacheSetIfNotExistsResponse cacheSetIfNotExistsResponse =
        target.setIfNotExists(cacheName, key, value, DEFAULT_TTL_SECONDS).join();

    assertThat(cacheSetIfNotExistsResponse).isInstanceOf(CacheSetIfNotExistsResponse.Error.class);

    // Without ttl specified
    cacheSetIfNotExistsResponse = target.setIfNotExists(cacheName, key, value).join();

    assertThat(cacheSetIfNotExistsResponse).isInstanceOf(CacheSetIfNotExistsResponse.Error.class);
  }

  @Test
  public void shouldFailSetValueToKeyWhenNullCacheName() {
    final String key = "test-key";
    final String value = "old-test-value";

    // With ttl specified
    CacheSetIfNotExistsResponse cacheSetIfNotExistsResponse =
        target.setIfNotExists(null, key, value, DEFAULT_TTL_SECONDS).join();

    assertThat(cacheSetIfNotExistsResponse).isInstanceOf(CacheSetIfNotExistsResponse.Error.class);

    // Without ttl specified
    cacheSetIfNotExistsResponse = target.setIfNotExists(null, key, value).join();

    assertThat(cacheSetIfNotExistsResponse).isInstanceOf(CacheSetIfNotExistsResponse.Error.class);
  }
}
