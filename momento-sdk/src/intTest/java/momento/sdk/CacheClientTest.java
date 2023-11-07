package momento.sdk;

import static momento.sdk.TestUtils.randomString;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

import momento.sdk.auth.CredentialProvider;
import momento.sdk.auth.StringCredentialProvider;
import momento.sdk.config.Configurations;
import momento.sdk.exceptions.AuthenticationException;
import momento.sdk.exceptions.InvalidArgumentException;
import momento.sdk.exceptions.NotFoundException;
import momento.sdk.exceptions.ServerUnavailableException;
import momento.sdk.responses.cache.DeleteResponse;
import momento.sdk.responses.cache.GetResponse;
import momento.sdk.responses.cache.IncrementResponse;
import momento.sdk.responses.cache.SetIfNotExistsResponse;
import momento.sdk.responses.cache.SetResponse;
import momento.sdk.responses.cache.control.CacheCreateResponse;
import momento.sdk.responses.cache.control.CacheFlushResponse;
import momento.sdk.responses.cache.ttl.ItemGetTtlResponse;
import momento.sdk.responses.cache.ttl.UpdateTtlResponse;
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

      final GetResponse getResponse = target.get(cacheName, key).join();
      assertThat(getResponse).isInstanceOf(GetResponse.Hit.class);
      assertThat(((GetResponse.Hit) getResponse).valueString()).isEqualTo(value);

      final DeleteResponse deleteResponse = target.delete(cacheName, key).join();
      assertThat(deleteResponse).isInstanceOf(DeleteResponse.Success.class);

      final GetResponse getAfterDeleteResponse = target.get(cacheName, key).join();
      assertThat(getAfterDeleteResponse).isInstanceOf(GetResponse.Miss.class);

      final GetResponse getForKeyInSomeOtherCache = target.get(alternateCacheName, key).join();
      assertThat(getForKeyInSomeOtherCache).isInstanceOf(GetResponse.Miss.class);
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
          .asInstanceOf(InstanceOfAssertFactories.type(SetResponse.Success.class))
          .satisfies(success -> assertThat(success.value()).isEqualTo(value));

      // Execute Flush
      assertThat(target.flushCache(cacheName))
          .succeedsWithin(FIVE_SECONDS)
          .isInstanceOf(CacheFlushResponse.Success.class);

      // Verify that previously set key is now a MISS
      assertThat(target.get(cacheName, key))
          .succeedsWithin(FIVE_SECONDS)
          .isInstanceOf(GetResponse.Miss.class);
    } finally {
      target.deleteCache(cacheName).join();
    }
  }

  @Test
  public void shouldReturnNotFoundWhenCacheToFlushDoesNotExist() {
    assertThat(target.flushCache(randomString("name")))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheFlushResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(NotFoundException.class));
  }

  @Test
  public void shouldReturnIllegalArgWhenCacheNameToFlushIsInvalid() {
    assertThat(target.flushCache(null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheFlushResponse.Error.class))
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
          .asInstanceOf(InstanceOfAssertFactories.type(CacheCreateResponse.Error.class))
          .satisfies(
              error -> assertThat(error).hasCauseInstanceOf(ServerUnavailableException.class));

      // But gets a valid response from Data plane
      assertThat(client.get("helloCache", "key"))
          .succeedsWithin(FIVE_SECONDS)
          .asInstanceOf(InstanceOfAssertFactories.type(GetResponse.Error.class))
          .satisfies(
              error -> {
                assertThat(error).hasCauseInstanceOf(AuthenticationException.class);
                assertThat(error.getTransportErrorDetails()).isNotEmpty();
              });

      assertThat(client.set("helloCache", "key", "value"))
          .succeedsWithin(FIVE_SECONDS)
          .asInstanceOf(InstanceOfAssertFactories.type(SetResponse.Error.class))
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
          .asInstanceOf(InstanceOfAssertFactories.type(CacheCreateResponse.Error.class))
          .satisfies(error -> assertThat(error).hasCauseInstanceOf(AuthenticationException.class));

      // Unable to reach data plane
      assertThat(client.set("helloCache", "key", "value"))
          .succeedsWithin(FIVE_SECONDS)
          .asInstanceOf(InstanceOfAssertFactories.type(SetResponse.Error.class))
          .satisfies(
              error -> assertThat(error).hasCauseInstanceOf(ServerUnavailableException.class));

      assertThat(client.get("helloCache", "key"))
          .succeedsWithin(FIVE_SECONDS)
          .asInstanceOf(InstanceOfAssertFactories.type(GetResponse.Error.class))
          .satisfies(
              error -> assertThat(error).hasCauseInstanceOf(ServerUnavailableException.class));
    }
  }

  @Test
  public void shouldUpdateTTLAndGetItWithStringKey() {
    final String key = "updateTTlGetTTLTestString";

    // set a key with default ttl
    SetResponse setResponse = target.set(cacheName, key, "value", DEFAULT_TTL_SECONDS).join();

    assertThat(setResponse).isInstanceOf(SetResponse.Success.class);

    ItemGetTtlResponse itemGetTtlResponse = target.itemGetTtl(cacheName, key).join();

    // retrieved ttl should work and less than default ttl
    assertThat(itemGetTtlResponse).isInstanceOf(ItemGetTtlResponse.Hit.class);
    assertThat(((ItemGetTtlResponse.Hit) itemGetTtlResponse).ttlMillis()).isLessThan(DEFAULT_TTL_SECONDS.toMillis());

    // update ttl to 300 seconds
    Duration updatedTTL = Duration.of(300, ChronoUnit.SECONDS);
    UpdateTtlResponse updateTtlResponse = target.updateTtl(cacheName, key, updatedTTL).join();

    assertThat(updateTtlResponse).isInstanceOf(UpdateTtlResponse.Set.class);

    itemGetTtlResponse = target.itemGetTtl(cacheName, key).join();

    // assert that the updated ttl is less than 300 seconds but more than 300 - epsilon (taken as 60 to reduce flakiness)
    assertThat(itemGetTtlResponse).isInstanceOf(ItemGetTtlResponse.Hit.class);
    assertThat(((ItemGetTtlResponse.Hit) itemGetTtlResponse).ttlMillis()).isLessThan(updatedTTL.toMillis());
    assertThat(((ItemGetTtlResponse.Hit) itemGetTtlResponse).ttlMillis()).isGreaterThan(updatedTTL.minusSeconds(60).toMillis());
  }

  @Test
  public void shouldUpdateTTLAndGetItWithByteArrayKey() {
    final byte[] key = "updateTTlGetTTLTestByteArray".getBytes();

    // set a key with default ttl
    SetResponse setResponse = target.set(cacheName, key, "value".getBytes(), DEFAULT_TTL_SECONDS).join();

    assertThat(setResponse).isInstanceOf(SetResponse.Success.class);

    ItemGetTtlResponse itemGetTtlResponse = target.itemGetTtl(cacheName, key).join();

    // retrieved ttl should work and less than default ttl
    assertThat(itemGetTtlResponse).isInstanceOf(ItemGetTtlResponse.Hit.class);
    assertThat(((ItemGetTtlResponse.Hit) itemGetTtlResponse).ttlMillis()).isLessThan(DEFAULT_TTL_SECONDS.toMillis());

    // update ttl to 300 seconds
    Duration updatedTTL = Duration.of(300, ChronoUnit.SECONDS);
    UpdateTtlResponse updateTtlResponse = target.updateTtl(cacheName, key, updatedTTL).join();

    assertThat(updateTtlResponse).isInstanceOf(UpdateTtlResponse.Set.class);

    itemGetTtlResponse = target.itemGetTtl(cacheName, key).join();

    // assert that the updated ttl is less than 300 seconds but more than 300 - epsilon (taken as 60 to reduce flakiness)
    assertThat(itemGetTtlResponse).isInstanceOf(ItemGetTtlResponse.Hit.class);
    assertThat(((ItemGetTtlResponse.Hit) itemGetTtlResponse).ttlMillis()).isLessThan(updatedTTL.toMillis());
    assertThat(((ItemGetTtlResponse.Hit) itemGetTtlResponse).ttlMillis()).isGreaterThan(updatedTTL.minusSeconds(60).toMillis());
  }

  @Test
  public void throwsOnUpdateTTLWhenNegative() {
    final byte[] key = "updateTTlGetTTLTestByteArray".getBytes();

    UpdateTtlResponse updateTtlResponse = target.updateTtl(cacheName, key, Duration.of(-1, ChronoUnit.SECONDS)).join();
    assertThat(updateTtlResponse).isInstanceOf(UpdateTtlResponse.Error.class);
    assertThat(((UpdateTtlResponse.Error) updateTtlResponse).getMessage()).contains("Cache item TTL cannot be negative");
  }

  @Test
  public void shouldReturnCacheIncrementedValuesWithStringField() {
    final String field = "field";

    IncrementResponse incrementResponse =
        target.increment(cacheName, field, 1, DEFAULT_TTL_SECONDS).join();

    assertThat(incrementResponse).isInstanceOf(IncrementResponse.Success.class);
    assertThat(((IncrementResponse.Success) incrementResponse).valueNumber()).isEqualTo(1);

    // increment with ttl specified
    incrementResponse = target.increment(cacheName, field, 50, DEFAULT_TTL_SECONDS).join();

    assertThat(incrementResponse).isInstanceOf(IncrementResponse.Success.class);
    assertThat(((IncrementResponse.Success) incrementResponse).valueNumber()).isEqualTo(51);

    // increment without ttl specified
    incrementResponse = target.increment(cacheName, field, -1051).join();

    assertThat(incrementResponse).isInstanceOf(IncrementResponse.Success.class);
    assertThat(((IncrementResponse.Success) incrementResponse).valueNumber()).isEqualTo(-1000);

    GetResponse getResp = target.get(cacheName, field).join();
    assertThat(getResp).isInstanceOf(GetResponse.Hit.class);
    assertThat(((GetResponse.Hit) getResp).valueString()).isEqualTo("-1000");
  }

  @Test
  public void shouldReturnCacheIncrementedValuesWithByteArrayField() {
    final byte[] field = "field".getBytes();

    IncrementResponse incrementResponse =
        target.increment(cacheName, field, 1, DEFAULT_TTL_SECONDS).join();

    assertThat(incrementResponse).isInstanceOf(IncrementResponse.Success.class);
    assertThat(((IncrementResponse.Success) incrementResponse).valueNumber()).isEqualTo(1);

    // increment with ttl specified
    incrementResponse = target.increment(cacheName, field, 50, DEFAULT_TTL_SECONDS).join();

    assertThat(incrementResponse).isInstanceOf(IncrementResponse.Success.class);
    assertThat(((IncrementResponse.Success) incrementResponse).valueNumber()).isEqualTo(51);

    // increment without ttl specified
    incrementResponse = target.increment(cacheName, field, -1051).join();

    assertThat(incrementResponse).isInstanceOf(IncrementResponse.Success.class);
    assertThat(((IncrementResponse.Success) incrementResponse).valueNumber()).isEqualTo(-1000);

    GetResponse getResp = target.get(cacheName, field).join();
    assertThat(getResp).isInstanceOf(GetResponse.Hit.class);
    assertThat(((GetResponse.Hit) getResp).valueString()).isEqualTo("-1000");
  }

  @Test
  public void shouldFailCacheIncrementedValuesWhenNullCacheName() {
    final String field = "field";

    // With ttl specified
    IncrementResponse incrementResponse =
        target.increment(null, field, 1, DEFAULT_TTL_SECONDS).join();
    assertThat(incrementResponse).isInstanceOf(IncrementResponse.Error.class);

    // Without ttl specified
    incrementResponse = target.increment(null, field, 1).join();
    assertThat(incrementResponse).isInstanceOf(IncrementResponse.Error.class);
  }

  @Test
  public void shouldFailCacheIncrementedValuesWhenCacheNotExist() {
    final String cacheName = "fake-cache";
    final String field = "field";

    // With ttl specified
    IncrementResponse incrementResponse =
        target.increment(cacheName, field, 1, DEFAULT_TTL_SECONDS).join();
    assertThat(incrementResponse).isInstanceOf(IncrementResponse.Error.class);

    // Without ttl specified
    incrementResponse = target.increment(cacheName, field, 1).join();
    assertThat(incrementResponse).isInstanceOf(IncrementResponse.Error.class);
  }

  @Test
  public void shouldSetStringValueToStringKeyWhenKeyNotExistsWithTtl() {
    final String key = randomString("test-key");
    final String value = randomString("test-value");

    SetIfNotExistsResponse setIfNotExistsResponse =
        target.setIfNotExists(cacheName, key, value, DEFAULT_TTL_SECONDS).join();

    assertThat(setIfNotExistsResponse).isInstanceOf(SetIfNotExistsResponse.Stored.class);
    assertThat(((SetIfNotExistsResponse.Stored) setIfNotExistsResponse).keyString()).isEqualTo(key);
    assertThat(((SetIfNotExistsResponse.Stored) setIfNotExistsResponse).valueString())
        .isEqualTo(value);

    GetResponse getResponse = target.get(cacheName, key).join();
    assertThat(getResponse).isInstanceOf(GetResponse.Hit.class);
    assertThat(((GetResponse.Hit) getResponse).valueString()).isEqualTo(value);
  }

  @Test
  public void shouldSetStringValueToStringKeyWhenKeyNotExistsWithoutTtl() {
    final String key = randomString("test-key");
    final String value = randomString("test-value");

    SetIfNotExistsResponse setIfNotExistsResponse =
        target.setIfNotExists(cacheName, key, value).join();

    assertThat(setIfNotExistsResponse).isInstanceOf(SetIfNotExistsResponse.Stored.class);
    assertThat(((SetIfNotExistsResponse.Stored) setIfNotExistsResponse).keyString()).isEqualTo(key);
    assertThat(((SetIfNotExistsResponse.Stored) setIfNotExistsResponse).valueString())
        .isEqualTo(value);

    GetResponse getResponse = target.get(cacheName, key).join();
    assertThat(getResponse).isInstanceOf(GetResponse.Hit.class);
    assertThat(((GetResponse.Hit) getResponse).valueString()).isEqualTo(value);
  }

  @Test
  public void shouldSetByteArrayValueToStringKeyWhenKeyNotExistsWithTtl() {
    final String key = randomString("test-key");
    final byte[] value = "test-value".getBytes();

    SetIfNotExistsResponse setIfNotExistsResponse =
        target.setIfNotExists(cacheName, key, value, DEFAULT_TTL_SECONDS).join();

    assertThat(setIfNotExistsResponse).isInstanceOf(SetIfNotExistsResponse.Stored.class);
    assertThat(((SetIfNotExistsResponse.Stored) setIfNotExistsResponse).keyString()).isEqualTo(key);
    assertThat(((SetIfNotExistsResponse.Stored) setIfNotExistsResponse).valueByteArray())
        .isEqualTo(value);

    GetResponse getResponse = target.get(cacheName, key).join();
    assertThat(getResponse).isInstanceOf(GetResponse.Hit.class);
    assertThat(((GetResponse.Hit) getResponse).valueByteArray()).isEqualTo(value);
  }

  @Test
  public void shouldSetByteArrayValueToStringKeyWhenKeyNotExistsWithoutTtl() {
    final String key = randomString("test-key");
    final byte[] value = "test-value".getBytes();

    SetIfNotExistsResponse setIfNotExistsResponse =
        target.setIfNotExists(cacheName, key, value).join();

    assertThat(setIfNotExistsResponse).isInstanceOf(SetIfNotExistsResponse.Stored.class);
    assertThat(((SetIfNotExistsResponse.Stored) setIfNotExistsResponse).keyString()).isEqualTo(key);
    assertThat(((SetIfNotExistsResponse.Stored) setIfNotExistsResponse).valueByteArray())
        .isEqualTo(value);

    GetResponse getResponse = target.get(cacheName, key).join();
    assertThat(getResponse).isInstanceOf(GetResponse.Hit.class);
    assertThat(((GetResponse.Hit) getResponse).valueByteArray()).isEqualTo(value);
  }

  @Test
  public void shouldSetStringValueToByteArrayKeyWhenKeyNotExistsWithTtl() {
    final byte[] key = "test-key".getBytes();
    final String value = "test-value";

    SetIfNotExistsResponse setIfNotExistsResponse =
        target.setIfNotExists(cacheName, key, value, DEFAULT_TTL_SECONDS).join();

    assertThat(setIfNotExistsResponse).isInstanceOf(SetIfNotExistsResponse.Stored.class);
    assertThat(((SetIfNotExistsResponse.Stored) setIfNotExistsResponse).keyByteArray())
        .isEqualTo(key);
    assertThat(((SetIfNotExistsResponse.Stored) setIfNotExistsResponse).valueString())
        .isEqualTo(value);

    GetResponse getResponse = target.get(cacheName, key).join();
    assertThat(getResponse).isInstanceOf(GetResponse.Hit.class);
    assertThat(((GetResponse.Hit) getResponse).valueString()).isEqualTo(value);
  }

  @Test
  public void shouldSetStringValueToByteArrayKeyWhenKeyNotExistsWithoutTtl() {
    final byte[] key = "test-key".getBytes();
    final String value = "test-value";

    SetIfNotExistsResponse setIfNotExistsResponse =
        target.setIfNotExists(cacheName, key, value).join();

    assertThat(setIfNotExistsResponse).isInstanceOf(SetIfNotExistsResponse.Stored.class);
    assertThat(((SetIfNotExistsResponse.Stored) setIfNotExistsResponse).keyByteArray())
        .isEqualTo(key);
    assertThat(((SetIfNotExistsResponse.Stored) setIfNotExistsResponse).valueString())
        .isEqualTo(value);

    GetResponse getResponse = target.get(cacheName, key).join();
    assertThat(getResponse).isInstanceOf(GetResponse.Hit.class);
    assertThat(((GetResponse.Hit) getResponse).valueString()).isEqualTo(value);
  }

  @Test
  public void shouldSetByteArrayValueToByteArrayKeyWhenKeyNotExistsWithttl() {
    final byte[] key = "test-key".getBytes();
    final byte[] value = "test-value".getBytes();

    SetIfNotExistsResponse setIfNotExistsResponse =
        target.setIfNotExists(cacheName, key, value, DEFAULT_TTL_SECONDS).join();

    assertThat(setIfNotExistsResponse).isInstanceOf(SetIfNotExistsResponse.Stored.class);
    assertThat(((SetIfNotExistsResponse.Stored) setIfNotExistsResponse).keyByteArray())
        .isEqualTo(key);
    assertThat(((SetIfNotExistsResponse.Stored) setIfNotExistsResponse).valueByteArray())
        .isEqualTo(value);

    GetResponse getResponse = target.get(cacheName, key).join();
    assertThat(getResponse).isInstanceOf(GetResponse.Hit.class);
    assertThat(((GetResponse.Hit) getResponse).valueByteArray()).isEqualTo(value);
  }

  @Test
  public void shouldSetByteArrayValueToByteArrayKeyWhenKeyNotExistsWithoutTtl() {
    final byte[] key = "test-key".getBytes();
    final byte[] value = "test-value".getBytes();

    SetIfNotExistsResponse setIfNotExistsResponse =
        target.setIfNotExists(cacheName, key, value).join();

    assertThat(setIfNotExistsResponse).isInstanceOf(SetIfNotExistsResponse.Stored.class);
    assertThat(((SetIfNotExistsResponse.Stored) setIfNotExistsResponse).keyByteArray())
        .isEqualTo(key);
    assertThat(((SetIfNotExistsResponse.Stored) setIfNotExistsResponse).valueByteArray())
        .isEqualTo(value);

    GetResponse getResponse = target.get(cacheName, key).join();
    assertThat(getResponse).isInstanceOf(GetResponse.Hit.class);
    assertThat(((GetResponse.Hit) getResponse).valueByteArray()).isEqualTo(value);
  }

  @Test
  public void shouldNotSetValueToKeyWhenKeyExists() {
    final String key = "test-key";
    final String oldValue = "old-test-value";
    final String newValue = "new-test-value";

    SetIfNotExistsResponse setIfNotExistsResponse =
        target.setIfNotExists(cacheName, key, oldValue, DEFAULT_TTL_SECONDS).join();

    assertThat(setIfNotExistsResponse).isInstanceOf(SetIfNotExistsResponse.Stored.class);
    assertThat(((SetIfNotExistsResponse.Stored) setIfNotExistsResponse).keyString()).isEqualTo(key);
    assertThat(((SetIfNotExistsResponse.Stored) setIfNotExistsResponse).valueString())
        .isEqualTo(oldValue);

    // When ttl is specified
    setIfNotExistsResponse =
        target.setIfNotExists(cacheName, key, newValue, DEFAULT_TTL_SECONDS).join();

    assertThat(setIfNotExistsResponse).isInstanceOf(SetIfNotExistsResponse.NotStored.class);

    GetResponse getResponse = target.get(cacheName, key).join();
    assertThat(getResponse).isInstanceOf(GetResponse.Hit.class);
    assertThat(((GetResponse.Hit) getResponse).valueString()).isEqualTo(oldValue);

    // When ttl is not specified
    setIfNotExistsResponse = target.setIfNotExists(cacheName, key, newValue).join();

    assertThat(setIfNotExistsResponse).isInstanceOf(SetIfNotExistsResponse.NotStored.class);

    getResponse = target.get(cacheName, key).join();
    assertThat(getResponse).isInstanceOf(GetResponse.Hit.class);
    assertThat(((GetResponse.Hit) getResponse).valueString()).isEqualTo(oldValue);
  }

  @Test
  public void shouldFailSetValueToKeyWhenCacheNotExist() {
    final String cacheName = "fake-cache";
    final String key = "test-key";
    final String value = "old-test-value";

    // With ttl specified
    SetIfNotExistsResponse setIfNotExistsResponse =
        target.setIfNotExists(cacheName, key, value, DEFAULT_TTL_SECONDS).join();

    assertThat(setIfNotExistsResponse).isInstanceOf(SetIfNotExistsResponse.Error.class);

    // Without ttl specified
    setIfNotExistsResponse = target.setIfNotExists(cacheName, key, value).join();

    assertThat(setIfNotExistsResponse).isInstanceOf(SetIfNotExistsResponse.Error.class);
  }

  @Test
  public void shouldFailSetValueToKeyWhenNullCacheName() {
    final String key = "test-key";
    final String value = "old-test-value";

    // With ttl specified
    SetIfNotExistsResponse setIfNotExistsResponse =
        target.setIfNotExists(null, key, value, DEFAULT_TTL_SECONDS).join();

    assertThat(setIfNotExistsResponse).isInstanceOf(SetIfNotExistsResponse.Error.class);

    // Without ttl specified
    setIfNotExistsResponse = target.setIfNotExists(null, key, value).join();

    assertThat(setIfNotExistsResponse).isInstanceOf(SetIfNotExistsResponse.Error.class);
  }
}
