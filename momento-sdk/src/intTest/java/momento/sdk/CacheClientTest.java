package momento.sdk;

import static momento.sdk.TestUtils.randomBytes;
import static momento.sdk.TestUtils.randomString;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import momento.sdk.auth.CredentialProvider;
import momento.sdk.auth.StringCredentialProvider;
import momento.sdk.config.Configurations;
import momento.sdk.exceptions.AuthenticationException;
import momento.sdk.exceptions.CacheNotFoundException;
import momento.sdk.exceptions.InvalidArgumentException;
import momento.sdk.exceptions.ServerUnavailableException;
import momento.sdk.responses.cache.DeleteResponse;
import momento.sdk.responses.cache.GetBatchResponse;
import momento.sdk.responses.cache.GetResponse;
import momento.sdk.responses.cache.IncrementResponse;
import momento.sdk.responses.cache.SetBatchResponse;
import momento.sdk.responses.cache.SetIfNotExistsResponse;
import momento.sdk.responses.cache.SetResponse;
import momento.sdk.responses.cache.control.CacheCreateResponse;
import momento.sdk.responses.cache.control.CacheFlushResponse;
import momento.sdk.responses.cache.ttl.ItemGetTtlResponse;
import momento.sdk.responses.cache.ttl.UpdateTtlResponse;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;

/** Just includes a happy test path that interacts with both control and data plane clients. */
final class CacheClientTest extends BaseTestClass {
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

  @Test
  public void createCacheGetSetDeleteValuesAndDeleteCache() {
    final String alternateCacheName = randomString("alternateName");
    final String key = randomString("key");
    final String value = randomString("value");

    cacheClient.createCache(alternateCacheName).join();
    try {
      cacheClient.set(cacheName, key, value).join();

      final GetResponse getResponse = cacheClient.get(cacheName, key).join();
      assertThat(getResponse).isInstanceOf(GetResponse.Hit.class);
      assertThat(((GetResponse.Hit) getResponse).valueString()).isEqualTo(value);

      final DeleteResponse deleteResponse = cacheClient.delete(cacheName, key).join();
      assertThat(deleteResponse).isInstanceOf(DeleteResponse.Success.class);

      final GetResponse getAfterDeleteResponse = cacheClient.get(cacheName, key).join();
      assertThat(getAfterDeleteResponse).isInstanceOf(GetResponse.Miss.class);

      final GetResponse getForKeyInSomeOtherCache = cacheClient.get(alternateCacheName, key).join();
      assertThat(getForKeyInSomeOtherCache).isInstanceOf(GetResponse.Miss.class);
    } finally {
      cacheClient.deleteCache(alternateCacheName).join();
    }
  }

  @Test
  public void shouldFlushCacheContents() {
    final String cacheToFlush = randomString("cacheToFlush");
    final String key = randomString();
    final String value = randomString();
    final Duration ttl1Hour = Duration.ofHours(1);

    try {
      CacheCreateResponse response = cacheClient.createCache(cacheToFlush).join();
      assertThat(response).isInstanceOf(CacheCreateResponse.Success.class);
      assertThat(cacheClient.set(cacheName, key, value, ttl1Hour))
          .succeedsWithin(FIVE_SECONDS)
          .asInstanceOf(InstanceOfAssertFactories.type(SetResponse.Success.class))
          .satisfies(success -> assertThat(success.value()).isEqualTo(value));

      // Execute Flush
      assertThat(cacheClient.flushCache(cacheName))
          .succeedsWithin(FIVE_SECONDS)
          .isInstanceOf(CacheFlushResponse.Success.class);

      // Verify that previously set key is now a MISS
      assertThat(cacheClient.get(cacheName, key))
          .succeedsWithin(FIVE_SECONDS)
          .isInstanceOf(GetResponse.Miss.class);
    } finally {
      cacheClient.deleteCache(cacheToFlush).join();
    }
  }

  @Test
  public void shouldReturnNotFoundWhenCacheToFlushDoesNotExist() {
    assertThat(cacheClient.flushCache(randomString("name")))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheFlushResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(CacheNotFoundException.class));
  }

  @Test
  public void shouldReturnIllegalArgWhenCacheNameToFlushIsInvalid() {
    assertThat(cacheClient.flushCache(null))
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
    final String key = randomString();

    // set a key with default ttl
    SetResponse setResponse = cacheClient.set(cacheName, key, "value", DEFAULT_TTL_SECONDS).join();

    assertThat(setResponse).isInstanceOf(SetResponse.Success.class);

    ItemGetTtlResponse itemGetTtlResponse = cacheClient.itemGetTtl(cacheName, key).join();

    // retrieved ttl should work and less than default ttl
    assertThat(itemGetTtlResponse).isInstanceOf(ItemGetTtlResponse.Hit.class);
    assertThat(((ItemGetTtlResponse.Hit) itemGetTtlResponse).remainingTtlMillis())
        .isLessThan(DEFAULT_TTL_SECONDS.toMillis());

    // update ttl to 300 seconds
    Duration updatedTTL = Duration.of(300, ChronoUnit.SECONDS);
    UpdateTtlResponse updateTtlResponse = cacheClient.updateTtl(cacheName, key, updatedTTL).join();

    assertThat(updateTtlResponse).isInstanceOf(UpdateTtlResponse.Set.class);

    itemGetTtlResponse = cacheClient.itemGetTtl(cacheName, key).join();

    // assert that the updated ttl is less than 300 seconds but more than 300 - epsilon (taken as 60
    // to reduce flakiness)
    assertThat(itemGetTtlResponse).isInstanceOf(ItemGetTtlResponse.Hit.class);
    assertThat(((ItemGetTtlResponse.Hit) itemGetTtlResponse).remainingTtlMillis())
        .isLessThan(updatedTTL.toMillis());
    assertThat(((ItemGetTtlResponse.Hit) itemGetTtlResponse).remainingTtlMillis())
        .isGreaterThan(updatedTTL.minusSeconds(60).toMillis());
  }

  @Test
  public void shouldUpdateTTLAndGetItWithByteArrayKey() {
    final byte[] key = randomBytes();

    // set a key with default ttl
    SetResponse setResponse =
        cacheClient.set(cacheName, key, "value".getBytes(), DEFAULT_TTL_SECONDS).join();

    assertThat(setResponse).isInstanceOf(SetResponse.Success.class);

    ItemGetTtlResponse itemGetTtlResponse = cacheClient.itemGetTtl(cacheName, key).join();

    // retrieved ttl should work and less than default ttl
    assertThat(itemGetTtlResponse).isInstanceOf(ItemGetTtlResponse.Hit.class);
    assertThat(((ItemGetTtlResponse.Hit) itemGetTtlResponse).remainingTtlMillis())
        .isLessThan(DEFAULT_TTL_SECONDS.toMillis());

    // update ttl to 300 seconds
    Duration updatedTTL = Duration.of(300, ChronoUnit.SECONDS);
    UpdateTtlResponse updateTtlResponse = cacheClient.updateTtl(cacheName, key, updatedTTL).join();

    assertThat(updateTtlResponse).isInstanceOf(UpdateTtlResponse.Set.class);

    itemGetTtlResponse = cacheClient.itemGetTtl(cacheName, key).join();

    // assert that the updated ttl is less than 300 seconds but more than 300 - epsilon (taken as 60
    // to reduce flakiness)
    assertThat(itemGetTtlResponse).isInstanceOf(ItemGetTtlResponse.Hit.class);
    assertThat(((ItemGetTtlResponse.Hit) itemGetTtlResponse).remainingTtlMillis())
        .isLessThan(updatedTTL.toMillis());
    assertThat(((ItemGetTtlResponse.Hit) itemGetTtlResponse).remainingTtlMillis())
        .isGreaterThan(updatedTTL.minusSeconds(60).toMillis());
  }

  @Test
  public void throwsOnUpdateTTLWhenNegative() {
    final byte[] key = randomBytes();

    UpdateTtlResponse updateTtlResponse =
        cacheClient.updateTtl(cacheName, key, Duration.of(-1, ChronoUnit.SECONDS)).join();
    assertThat(updateTtlResponse).isInstanceOf(UpdateTtlResponse.Error.class);
    assertThat(((UpdateTtlResponse.Error) updateTtlResponse).getMessage())
        .contains("Cache item TTL cannot be negative");
  }

  @Test
  public void shouldReturnCacheIncrementedValuesWithStringField() {
    final String field = randomString();

    IncrementResponse incrementResponse =
        cacheClient.increment(cacheName, field, 1, DEFAULT_TTL_SECONDS).join();

    assertThat(incrementResponse).isInstanceOf(IncrementResponse.Success.class);
    assertThat(((IncrementResponse.Success) incrementResponse).valueNumber()).isEqualTo(1);

    // increment with ttl specified
    incrementResponse = cacheClient.increment(cacheName, field, 50, DEFAULT_TTL_SECONDS).join();

    assertThat(incrementResponse).isInstanceOf(IncrementResponse.Success.class);
    assertThat(((IncrementResponse.Success) incrementResponse).valueNumber()).isEqualTo(51);

    // increment without ttl specified
    incrementResponse = cacheClient.increment(cacheName, field, -1051).join();

    assertThat(incrementResponse).isInstanceOf(IncrementResponse.Success.class);
    assertThat(((IncrementResponse.Success) incrementResponse).valueNumber()).isEqualTo(-1000);

    GetResponse getResp = cacheClient.get(cacheName, field).join();
    assertThat(getResp).isInstanceOf(GetResponse.Hit.class);
    assertThat(((GetResponse.Hit) getResp).valueString()).isEqualTo("-1000");
  }

  @Test
  public void shouldReturnCacheIncrementedValuesWithByteArrayField() {
    final byte[] field = randomBytes();

    IncrementResponse incrementResponse =
        cacheClient.increment(cacheName, field, 1, DEFAULT_TTL_SECONDS).join();

    assertThat(incrementResponse).isInstanceOf(IncrementResponse.Success.class);
    assertThat(((IncrementResponse.Success) incrementResponse).valueNumber()).isEqualTo(1);

    // increment with ttl specified
    incrementResponse = cacheClient.increment(cacheName, field, 50, DEFAULT_TTL_SECONDS).join();

    assertThat(incrementResponse).isInstanceOf(IncrementResponse.Success.class);
    assertThat(((IncrementResponse.Success) incrementResponse).valueNumber()).isEqualTo(51);

    // increment without ttl specified
    incrementResponse = cacheClient.increment(cacheName, field, -1051).join();

    assertThat(incrementResponse).isInstanceOf(IncrementResponse.Success.class);
    assertThat(((IncrementResponse.Success) incrementResponse).valueNumber()).isEqualTo(-1000);

    GetResponse getResp = cacheClient.get(cacheName, field).join();
    assertThat(getResp).isInstanceOf(GetResponse.Hit.class);
    assertThat(((GetResponse.Hit) getResp).valueString()).isEqualTo("-1000");
  }

  @Test
  public void shouldFailCacheIncrementedValuesWhenNullCacheName() {
    final String field = randomString();

    // With ttl specified
    IncrementResponse incrementResponse =
        cacheClient.increment(null, field, 1, DEFAULT_TTL_SECONDS).join();
    assertThat(incrementResponse).isInstanceOf(IncrementResponse.Error.class);

    // Without ttl specified
    incrementResponse = cacheClient.increment(null, field, 1).join();
    assertThat(incrementResponse).isInstanceOf(IncrementResponse.Error.class);
  }

  @Test
  public void shouldFailCacheIncrementedValuesWhenCacheNotExist() {
    final String cacheName = randomString("fake-cache");
    final String field = randomString();

    // With ttl specified
    IncrementResponse incrementResponse =
        cacheClient.increment(cacheName, field, 1, DEFAULT_TTL_SECONDS).join();
    assertThat(incrementResponse).isInstanceOf(IncrementResponse.Error.class);

    // Without ttl specified
    incrementResponse = cacheClient.increment(cacheName, field, 1).join();
    assertThat(incrementResponse).isInstanceOf(IncrementResponse.Error.class);
  }

  @Test
  public void shouldSetStringValueToStringKeyWhenKeyNotExistsWithTtl() {
    final String key = randomString();
    final String value = randomString();

    SetIfNotExistsResponse setIfNotExistsResponse =
        cacheClient.setIfNotExists(cacheName, key, value, DEFAULT_TTL_SECONDS).join();

    assertThat(setIfNotExistsResponse).isInstanceOf(SetIfNotExistsResponse.Stored.class);
    assertThat(((SetIfNotExistsResponse.Stored) setIfNotExistsResponse).keyString()).isEqualTo(key);
    assertThat(((SetIfNotExistsResponse.Stored) setIfNotExistsResponse).valueString())
        .isEqualTo(value);

    GetResponse getResponse = cacheClient.get(cacheName, key).join();
    assertThat(getResponse).isInstanceOf(GetResponse.Hit.class);
    assertThat(((GetResponse.Hit) getResponse).valueString()).isEqualTo(value);
  }

  @Test
  public void shouldSetStringValueToStringKeyWhenKeyNotExistsWithoutTtl() {
    final String key = randomString();
    final String value = randomString();

    SetIfNotExistsResponse setIfNotExistsResponse =
        cacheClient.setIfNotExists(cacheName, key, value).join();

    assertThat(setIfNotExistsResponse).isInstanceOf(SetIfNotExistsResponse.Stored.class);
    assertThat(((SetIfNotExistsResponse.Stored) setIfNotExistsResponse).keyString()).isEqualTo(key);
    assertThat(((SetIfNotExistsResponse.Stored) setIfNotExistsResponse).valueString())
        .isEqualTo(value);

    GetResponse getResponse = cacheClient.get(cacheName, key).join();
    assertThat(getResponse).isInstanceOf(GetResponse.Hit.class);
    assertThat(((GetResponse.Hit) getResponse).valueString()).isEqualTo(value);
  }

  @Test
  public void shouldSetByteArrayValueToStringKeyWhenKeyNotExistsWithTtl() {
    final String key = randomString();
    final byte[] value = randomBytes();

    SetIfNotExistsResponse setIfNotExistsResponse =
        cacheClient.setIfNotExists(cacheName, key, value, DEFAULT_TTL_SECONDS).join();

    assertThat(setIfNotExistsResponse).isInstanceOf(SetIfNotExistsResponse.Stored.class);
    assertThat(((SetIfNotExistsResponse.Stored) setIfNotExistsResponse).keyString()).isEqualTo(key);
    assertThat(((SetIfNotExistsResponse.Stored) setIfNotExistsResponse).valueByteArray())
        .isEqualTo(value);

    GetResponse getResponse = cacheClient.get(cacheName, key).join();
    assertThat(getResponse).isInstanceOf(GetResponse.Hit.class);
    assertThat(((GetResponse.Hit) getResponse).valueByteArray()).isEqualTo(value);
  }

  @Test
  public void shouldSetByteArrayValueToStringKeyWhenKeyNotExistsWithoutTtl() {
    final String key = randomString();
    final byte[] value = randomBytes();

    SetIfNotExistsResponse setIfNotExistsResponse =
        cacheClient.setIfNotExists(cacheName, key, value).join();

    assertThat(setIfNotExistsResponse).isInstanceOf(SetIfNotExistsResponse.Stored.class);
    assertThat(((SetIfNotExistsResponse.Stored) setIfNotExistsResponse).keyString()).isEqualTo(key);
    assertThat(((SetIfNotExistsResponse.Stored) setIfNotExistsResponse).valueByteArray())
        .isEqualTo(value);

    GetResponse getResponse = cacheClient.get(cacheName, key).join();
    assertThat(getResponse).isInstanceOf(GetResponse.Hit.class);
    assertThat(((GetResponse.Hit) getResponse).valueByteArray()).isEqualTo(value);
  }

  @Test
  public void shouldSetStringValueToByteArrayKeyWhenKeyNotExistsWithTtl() {
    final byte[] key = randomBytes();
    final String value = randomString();

    SetIfNotExistsResponse setIfNotExistsResponse =
        cacheClient.setIfNotExists(cacheName, key, value, DEFAULT_TTL_SECONDS).join();

    assertThat(setIfNotExistsResponse).isInstanceOf(SetIfNotExistsResponse.Stored.class);
    assertThat(((SetIfNotExistsResponse.Stored) setIfNotExistsResponse).keyByteArray())
        .isEqualTo(key);
    assertThat(((SetIfNotExistsResponse.Stored) setIfNotExistsResponse).valueString())
        .isEqualTo(value);

    GetResponse getResponse = cacheClient.get(cacheName, key).join();
    assertThat(getResponse).isInstanceOf(GetResponse.Hit.class);
    assertThat(((GetResponse.Hit) getResponse).valueString()).isEqualTo(value);
  }

  @Test
  public void shouldSetStringValueToByteArrayKeyWhenKeyNotExistsWithoutTtl() {
    final byte[] key = randomBytes();
    final String value = randomString();

    SetIfNotExistsResponse setIfNotExistsResponse =
        cacheClient.setIfNotExists(cacheName, key, value).join();

    assertThat(setIfNotExistsResponse).isInstanceOf(SetIfNotExistsResponse.Stored.class);
    assertThat(((SetIfNotExistsResponse.Stored) setIfNotExistsResponse).keyByteArray())
        .isEqualTo(key);
    assertThat(((SetIfNotExistsResponse.Stored) setIfNotExistsResponse).valueString())
        .isEqualTo(value);

    GetResponse getResponse = cacheClient.get(cacheName, key).join();
    assertThat(getResponse).isInstanceOf(GetResponse.Hit.class);
    assertThat(((GetResponse.Hit) getResponse).valueString()).isEqualTo(value);
  }

  @Test
  public void shouldSetByteArrayValueToByteArrayKeyWhenKeyNotExistsWithttl() {
    final byte[] key = randomBytes();
    final byte[] value = randomBytes();

    SetIfNotExistsResponse setIfNotExistsResponse =
        cacheClient.setIfNotExists(cacheName, key, value, DEFAULT_TTL_SECONDS).join();

    assertThat(setIfNotExistsResponse).isInstanceOf(SetIfNotExistsResponse.Stored.class);
    assertThat(((SetIfNotExistsResponse.Stored) setIfNotExistsResponse).keyByteArray())
        .isEqualTo(key);
    assertThat(((SetIfNotExistsResponse.Stored) setIfNotExistsResponse).valueByteArray())
        .isEqualTo(value);

    GetResponse getResponse = cacheClient.get(cacheName, key).join();
    assertThat(getResponse).isInstanceOf(GetResponse.Hit.class);
    assertThat(((GetResponse.Hit) getResponse).valueByteArray()).isEqualTo(value);
  }

  @Test
  public void shouldSetByteArrayValueToByteArrayKeyWhenKeyNotExistsWithoutTtl() {
    final byte[] key = randomBytes();
    final byte[] value = randomBytes();

    SetIfNotExistsResponse setIfNotExistsResponse =
        cacheClient.setIfNotExists(cacheName, key, value).join();

    assertThat(setIfNotExistsResponse).isInstanceOf(SetIfNotExistsResponse.Stored.class);
    assertThat(((SetIfNotExistsResponse.Stored) setIfNotExistsResponse).keyByteArray())
        .isEqualTo(key);
    assertThat(((SetIfNotExistsResponse.Stored) setIfNotExistsResponse).valueByteArray())
        .isEqualTo(value);

    GetResponse getResponse = cacheClient.get(cacheName, key).join();
    assertThat(getResponse).isInstanceOf(GetResponse.Hit.class);
    assertThat(((GetResponse.Hit) getResponse).valueByteArray()).isEqualTo(value);
  }

  @Test
  public void shouldNotSetValueToKeyWhenKeyExists() {
    final String key = randomString();
    final String oldValue = "old-test-value";
    final String newValue = "new-test-value";

    SetIfNotExistsResponse setIfNotExistsResponse =
        cacheClient.setIfNotExists(cacheName, key, oldValue, DEFAULT_TTL_SECONDS).join();

    assertThat(setIfNotExistsResponse).isInstanceOf(SetIfNotExistsResponse.Stored.class);
    assertThat(((SetIfNotExistsResponse.Stored) setIfNotExistsResponse).keyString()).isEqualTo(key);
    assertThat(((SetIfNotExistsResponse.Stored) setIfNotExistsResponse).valueString())
        .isEqualTo(oldValue);

    // When ttl is specified
    setIfNotExistsResponse =
        cacheClient.setIfNotExists(cacheName, key, newValue, DEFAULT_TTL_SECONDS).join();

    assertThat(setIfNotExistsResponse).isInstanceOf(SetIfNotExistsResponse.NotStored.class);

    GetResponse getResponse = cacheClient.get(cacheName, key).join();
    assertThat(getResponse).isInstanceOf(GetResponse.Hit.class);
    assertThat(((GetResponse.Hit) getResponse).valueString()).isEqualTo(oldValue);

    // When ttl is not specified
    setIfNotExistsResponse = cacheClient.setIfNotExists(cacheName, key, newValue).join();

    assertThat(setIfNotExistsResponse).isInstanceOf(SetIfNotExistsResponse.NotStored.class);

    getResponse = cacheClient.get(cacheName, key).join();
    assertThat(getResponse).isInstanceOf(GetResponse.Hit.class);
    assertThat(((GetResponse.Hit) getResponse).valueString()).isEqualTo(oldValue);
  }

  @Test
  public void shouldFailSetValueToKeyWhenCacheNotExist() {
    final String cacheName = "fake-cache";
    final String key = randomString();
    final String value = "old-test-value";

    // With ttl specified
    SetIfNotExistsResponse setIfNotExistsResponse =
        cacheClient.setIfNotExists(cacheName, key, value, DEFAULT_TTL_SECONDS).join();

    assertThat(setIfNotExistsResponse).isInstanceOf(SetIfNotExistsResponse.Error.class);

    // Without ttl specified
    setIfNotExistsResponse = cacheClient.setIfNotExists(cacheName, key, value).join();

    assertThat(setIfNotExistsResponse).isInstanceOf(SetIfNotExistsResponse.Error.class);
  }

  @Test
  public void shouldFailSetValueToKeyWhenNullCacheName() {
    final String key = randomString();
    final String value = "old-test-value";

    // With ttl specified
    SetIfNotExistsResponse setIfNotExistsResponse =
        cacheClient.setIfNotExists(null, key, value, DEFAULT_TTL_SECONDS).join();

    assertThat(setIfNotExistsResponse).isInstanceOf(SetIfNotExistsResponse.Error.class);

    // Without ttl specified
    setIfNotExistsResponse = cacheClient.setIfNotExists(null, key, value).join();

    assertThat(setIfNotExistsResponse).isInstanceOf(SetIfNotExistsResponse.Error.class);
  }

  @Test
  public void getBatchSetBatchHappyPath() {
    final Map<String, String> items = new HashMap<>();
    items.put("key1", "val1");
    items.put("key2", "val2");
    items.put("key3", "val3");
    final SetBatchResponse setBatchResponse =
        cacheClient.setBatch(cacheName, items, Duration.ofMinutes(1)).join();
    assertThat(setBatchResponse).isInstanceOf(SetBatchResponse.Success.class);
    for (SetResponse setResponse :
        ((SetBatchResponse.Success) setBatchResponse).results().values()) {
      assertThat(setResponse).isInstanceOf(SetResponse.Success.class);
    }

    final GetBatchResponse getBatchResponse =
        cacheClient.getBatch(cacheName, items.keySet()).join();

    assertThat(getBatchResponse).isInstanceOf(GetBatchResponse.Success.class);
    assertThat(((GetBatchResponse.Success) getBatchResponse).valueMapStringString())
        .containsExactlyEntriesOf(items);
  }

  @Test
  public void getBatchFailsWithNullCacheName() {
    assertThat(cacheClient.getBatch(null, new ArrayList<>()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(GetBatchResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void getBatchFailsWithNonExistentCache() {
    final List<String> items = new ArrayList<>();
    items.add("key1");

    assertThat(cacheClient.getBatch(randomString("cache"), items))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(GetBatchResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(CacheNotFoundException.class));
  }

  @Test
  public void setBatchFailsWithNullCacheName() {
    assertThat(cacheClient.setBatch(null, new HashMap<>()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SetBatchResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void setBatchFailsWithNonExistentCache() {
    final Map<String, String> items = new HashMap<>();
    items.put("key1", "val1");

    assertThat(cacheClient.setBatch(randomString("cache"), items))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SetBatchResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(CacheNotFoundException.class));
  }

  @Test
  public void getBatchSetBatchStringBytesHappyPath() {
    final Map<String, byte[]> items = new HashMap<>();
    items.put("key1", "val1".getBytes());
    items.put("key2", "val2".getBytes());
    items.put("key3", "val3".getBytes());
    final SetBatchResponse setBatchResponse =
        cacheClient.setBatchStringBytes(cacheName, items, Duration.ofMinutes(1)).join();
    assertThat(setBatchResponse).isInstanceOf(SetBatchResponse.Success.class);
    for (SetResponse setResponse :
        ((SetBatchResponse.Success) setBatchResponse).results().values()) {
      assertThat(setResponse).isInstanceOf(SetResponse.Success.class);
    }

    final GetBatchResponse getBatchResponse =
        cacheClient.getBatch(cacheName, items.keySet()).join();

    assertThat(getBatchResponse).isInstanceOf(GetBatchResponse.Success.class);
    assertThat(((GetBatchResponse.Success) getBatchResponse).valueMapStringByteArray())
        .containsExactlyEntriesOf(items);
  }

  @Test
  public void setBatchStringBytesFailsWithNullCacheName() {
    assertThat(cacheClient.setBatchStringBytes(null, new HashMap<>()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SetBatchResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void setBatchStringBytessFailsWithNonExistentCache() {
    final Map<String, byte[]> items = new HashMap<>();
    items.put("key1", "val1".getBytes());

    assertThat(cacheClient.setBatchStringBytes(randomString("cache"), items))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SetBatchResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(CacheNotFoundException.class));
  }
}
