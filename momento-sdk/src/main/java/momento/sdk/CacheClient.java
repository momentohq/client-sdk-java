package momento.sdk;

import io.opentelemetry.api.OpenTelemetry;
import java.io.Closeable;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import momento.sdk.messages.CacheDeleteResponse;
import momento.sdk.messages.CacheGetResponse;
import momento.sdk.messages.CacheIncrementResponse;
import momento.sdk.messages.CacheSetIfNotExistsResponse;
import momento.sdk.messages.CacheSetResponse;
import momento.sdk.messages.CreateCacheResponse;
import momento.sdk.messages.CreateSigningKeyResponse;
import momento.sdk.messages.DeleteCacheResponse;
import momento.sdk.messages.FlushCacheResponse;
import momento.sdk.messages.ListCachesResponse;
import momento.sdk.messages.ListSigningKeysResponse;
import momento.sdk.messages.RevokeSigningKeyResponse;

/** Client to perform operations against the Momento Cache Service */
public final class CacheClient implements Closeable {

  private final ScsControlClient scsControlClient;
  private final ScsDataClient scsDataClient;

  CacheClient(
      String authToken,
      Duration itemDefaultTtl,
      Optional<OpenTelemetry> telemetryOptional,
      Optional<Duration> requestTimeout) {
    MomentoEndpointsResolver.MomentoEndpoints endpoints =
        MomentoEndpointsResolver.resolve(authToken, Optional.empty());
    this.scsControlClient = new ScsControlClient(authToken, endpoints.controlEndpoint());
    this.scsDataClient =
        new ScsDataClient(
            authToken,
            endpoints.cacheEndpoint(),
            itemDefaultTtl,
            telemetryOptional,
            requestTimeout);
  }

  public static CacheClientBuilder builder(String authToken, Duration itemDefaultTtl) {
    return new CacheClientBuilder(authToken, itemDefaultTtl);
  }

  /**
   * Creates a cache with provided name
   *
   * @param cacheName Name of the cache to be created.
   * @return The result of the create cache operation
   */
  public CreateCacheResponse createCache(String cacheName) {
    return scsControlClient.createCache(cacheName);
  }

  /**
   * Deletes a cache
   *
   * @param cacheName The name of the cache to be deleted.
   * @return The result of the cache deletion operation.
   */
  public DeleteCacheResponse deleteCache(String cacheName) {
    return scsControlClient.deleteCache(cacheName);
  }

  /**
   * Flushes the contents of the cache.
   *
   * @param cacheName The name of the cache to be flushed.
   * @return The result of the cache flush operation.
   */
  public FlushCacheResponse flushCache(String cacheName) {
    return scsControlClient.flushCache(cacheName);
  }

  /** Lists all caches. */
  public ListCachesResponse listCaches() {
    return scsControlClient.listCaches();
  }

  /**
   * Creates a new Momento signing key
   *
   * @param ttl The key's time-to-live duration
   * @return The created key and its metadata
   */
  public CreateSigningKeyResponse createSigningKey(Duration ttl) {
    return scsControlClient.createSigningKey(ttl, scsDataClient.getEndpoint());
  }

  /**
   * Revokes a Momento signing key, all tokens signed by which will be invalid
   *
   * @param keyId The id of the key to revoke
   */
  public RevokeSigningKeyResponse revokeSigningKey(String keyId) {
    return scsControlClient.revokeSigningKey(keyId);
  }

  /**
   * Lists all Momento signing keys.
   *
   * @return A list of Momento signing keys along with a pagination token (if present)
   */
  public ListSigningKeysResponse listSigningKeys() {
    return scsControlClient.listSigningKeys(scsDataClient.getEndpoint());
  }

  /**
   * Get the cache value stored for the given key.
   *
   * @param cacheName Name of the cache to get the item from
   * @param key The key to get
   * @return Future with {@link CacheGetResponse} containing the status of the get operation and the
   *     associated value data.
   */
  public CompletableFuture<CacheGetResponse> get(String cacheName, byte[] key) {
    return scsDataClient.get(cacheName, key);
  }

  /**
   * Get the cache value stored for the given key.
   *
   * @param cacheName Name of the cache to get the item from
   * @param key The key to get
   * @return Future with {@link CacheGetResponse} containing the status of the get operation and the
   *     associated value data.
   */
  public CompletableFuture<CacheGetResponse> get(String cacheName, String key) {
    return scsDataClient.get(cacheName, key);
  }

  /**
   * Delete the value stored in Momento cache.
   *
   * @param cacheName Name of the cache to delete the item from
   * @param key The key to delete
   * @return Future with {@link CacheDeleteResponse}
   */
  public CompletableFuture<CacheDeleteResponse> delete(String cacheName, String key) {
    return scsDataClient.delete(cacheName, key);
  }

  /**
   * Delete the value stored in Momento cache.
   *
   * @param cacheName Name of the cache to delete the item from
   * @param key The key to delete
   * @return Future with {@link CacheDeleteResponse}
   */
  public CompletableFuture<CacheDeleteResponse> delete(String cacheName, byte[] key) {
    return scsDataClient.delete(cacheName, key);
  }

  /**
   * Sets the value in cache with a given Time To Live (TTL) seconds.
   *
   * <p>If a value for this key is already present it will be replaced by the new value.
   *
   * @param cacheName Name of the cache to store the item in
   * @param key The key under which the value is to be added.
   * @param value The value to be stored.
   * @param ttl Time to Live for the item in Cache. This ttl takes precedence over the TTL used when
   *     building a cache client {@link CacheClient#builder(String, Duration)}
   * @return Future containing the result of the set operation.
   */
  public CompletableFuture<CacheSetResponse> set(
      String cacheName, String key, ByteBuffer value, Duration ttl) {
    return scsDataClient.set(cacheName, key, value, ttl);
  }

  /**
   * Sets the value in the cache. If a value for this key is already present it will be replaced by
   * the new value.
   *
   * <p>The Time to Live (TTL) seconds defaults to the parameter used when building this Cache
   * client - {@link CacheClient#builder(String, Duration)}
   *
   * @param cacheName Name of the cache to store the item in
   * @param key The key under which the value is to be added.
   * @param value The value to be stored.
   * @return Future containing the result of the set operation.
   */
  public CompletableFuture<CacheSetResponse> set(String cacheName, String key, ByteBuffer value) {
    return scsDataClient.set(cacheName, key, value);
  }

  /**
   * Sets the value in cache with a given Time To Live (TTL) seconds.
   *
   * <p>If a value for this key is already present it will be replaced by the new value.
   *
   * @param cacheName Name of the cache to store the item in
   * @param key The key under which the value is to be added.
   * @param value The value to be stored.
   * @param ttl Time to Live for the item in Cache. This ttl takes precedence over the TTL used when
   *     building a cache client {@link CacheClient#builder(String, Duration)}
   * @return Future containing the result of the set operation.
   */
  public CompletableFuture<CacheSetResponse> set(
      String cacheName, byte[] key, byte[] value, Duration ttl) {
    return scsDataClient.set(cacheName, key, value, ttl);
  }

  /**
   * Sets the value in the cache. If a value for this key is already present it will be replaced by
   * the new value.
   *
   * <p>The Time to Live (TTL) seconds defaults to the parameter used when building this Cache
   * client - {@link CacheClient#builder(String, Duration)}
   *
   * @param cacheName Name of the cache to store the item in
   * @param key The key under which the value is to be added.
   * @param value The value to be stored.
   * @return Future containing the result of the set operation.
   */
  public CompletableFuture<CacheSetResponse> set(String cacheName, byte[] key, byte[] value) {
    return scsDataClient.set(cacheName, key, value);
  }

  /**
   * Sets the value in cache with a given Time To Live (TTL) seconds.
   *
   * <p>If a value for this key is already present it will be replaced by the new value.
   *
   * @param cacheName Name of the cache to store the item in
   * @param key The key under which the value is to be added.
   * @param value The value to be stored.
   * @param ttl Time to Live for the item in Cache. This ttl takes precedence over the TTL used when
   *     building a cache client {@link CacheClient#builder(String, Duration)}
   * @return Future containing the result of the set operation.
   */
  public CompletableFuture<CacheSetResponse> set(
      String cacheName, String key, String value, Duration ttl) {
    return scsDataClient.set(cacheName, key, value, ttl);
  }

  /**
   * Sets the value in the cache. If a value for this key is already present it will be replaced by
   * the new value.
   *
   * <p>The Time to Live (TTL) seconds defaults to the parameter used when building this Cache
   * client - {@link CacheClient#builder(String, Duration)}
   *
   * @param cacheName Name of the cache to store the item in
   * @param key The key under which the value is to be added.
   * @param value The value to be stored.
   * @return Future containing the result of the set operation.
   */
  public CompletableFuture<CacheSetResponse> set(String cacheName, String key, String value) {
    return scsDataClient.set(cacheName, key, value);
  }

  /**
   * Associates a key with a value. If a value for this key is already present it is not replaced by
   * the new value.
   *
   * @param cacheName Name of the cache to store the item in
   * @param key {String} The key under which the value is to be added.
   * @param value {String} The value to be stored.
   * @param ttl Time to Live for the item in Cache. This ttl takes precedence over the TTL used when
   *     building a cache client {@link CacheClient#builder(String, Duration)}
   * @return Future containing the result of the set operation.
   */
  public CompletableFuture<CacheSetIfNotExistsResponse> setIfNotExists(
      String cacheName, String key, String value, Duration ttl) {
    return scsDataClient.setIfNotExists(cacheName, key, value, ttl);
  }

  /**
   * Associates a key with a value. If a value for this key is already present it is not replaced by
   * the new value.
   *
   * @param cacheName Name of the cache to store the item in
   * @param key {String} The key under which the value is to be added.
   * @param value {Byte Array} The value to be stored.
   * @param ttl Time to Live for the item in Cache. This ttl takes precedence over the TTL used when
   *     building a cache client {@link CacheClient#builder(String, Duration)}
   * @return Future containing the result of the set operation.
   */
  public CompletableFuture<CacheSetIfNotExistsResponse> setIfNotExists(
      String cacheName, String key, byte[] value, Duration ttl) {
    return scsDataClient.setIfNotExists(cacheName, key, value, ttl);
  }

  /**
   * Associates a key with a value. If a value for this key is already present it is not replaced by
   * the new value.
   *
   * @param cacheName Name of the cache to store the item in
   * @param key {Byte Array} The key under which the value is to be added.
   * @param value {String} The value to be stored.
   * @param ttl Time to Live for the item in Cache. This ttl takes precedence over the TTL used when
   *     building a cache client {@link CacheClient#builder(String, Duration)}
   * @return Future containing the result of the set operation.
   */
  public CompletableFuture<CacheSetIfNotExistsResponse> setIfNotExists(
      String cacheName, byte[] key, String value, Duration ttl) {
    return scsDataClient.setIfNotExists(cacheName, key, value, ttl);
  }

  /**
   * Associates a key with a value. If a value for this key is already present it is not replaced by
   * the new value.
   *
   * @param cacheName Name of the cache to store the item in
   * @param key {Byte Array} The key under which the value is to be added.
   * @param value {Byte Array} The value to be stored.
   * @param ttl Time to Live for the item in Cache. This ttl takes precedence over the TTL used when
   *     building a cache client {@link CacheClient#builder(String, Duration)}
   * @return Future containing the result of the set operation.
   */
  public CompletableFuture<CacheSetIfNotExistsResponse> setIfNotExists(
      String cacheName, byte[] key, byte[] value, Duration ttl) {
    return scsDataClient.setIfNotExists(cacheName, key, value, ttl);
  }

  /**
   * Increments the value in the cache by an amount.
   *
   * @param cacheName Name of the cache to store the item in
   * @param field The field under which the value is to be added.
   * @param amount The amount by which the cache value is to be incremented.
   * @param ttl Time to Live for the item in Cache. This ttl takes precedence over the TTL used when
   *     building a cache client {@link CacheClient#builder(String, Duration)}
   * @return Future containing the result of the increment operation.
   */
  public CompletableFuture<CacheIncrementResponse> increment(
      String cacheName, String field, long amount, Duration ttl) {
    return scsDataClient.increment(cacheName, field, amount, ttl);
  }

  /**
   * Increments the value in the cache by an amount.
   *
   * @param cacheName Name of the cache to store the item in
   * @param field The field under which the value is to be added.
   * @param amount The amount by which the cache value is to be incremented.
   * @param ttl Time to Live for the item in Cache. This ttl takes precedence over the TTL used when
   *     building a cache client {@link CacheClient#builder(String, Duration)}
   * @return Future containing the result of the increment operation.
   */
  public CompletableFuture<CacheIncrementResponse> increment(
      String cacheName, byte[] field, long amount, Duration ttl) {
    return scsDataClient.increment(cacheName, field, amount, ttl);
  }

  @Override
  public void close() {
    scsControlClient.close();
    scsDataClient.close();
  }
}
