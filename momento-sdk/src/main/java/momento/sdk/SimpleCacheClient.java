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
import momento.sdk.messages.CacheSetResponse;
import momento.sdk.messages.CreateCacheResponse;
import momento.sdk.messages.CreateSigningKeyResponse;
import momento.sdk.messages.DeleteCacheResponse;
import momento.sdk.messages.FlushCacheResponse;
import momento.sdk.messages.ListCachesResponse;
import momento.sdk.messages.ListSigningKeysResponse;
import momento.sdk.messages.RevokeSigningKeyResponse;

/** Client to perform operations against the Simple Cache Service */
public final class SimpleCacheClient implements Closeable {

  private final ScsControlClient scsControlClient;
  private final ScsDataClient scsDataClient;

  SimpleCacheClient(
      String authToken,
      long itemDefaultTtlSeconds,
      Optional<OpenTelemetry> telemetryOptional,
      Optional<Duration> requestTimeout) {
    MomentoEndpointsResolver.MomentoEndpoints endpoints =
        MomentoEndpointsResolver.resolve(authToken, Optional.empty());
    this.scsControlClient = new ScsControlClient(authToken, endpoints.controlEndpoint());
    this.scsDataClient =
        new ScsDataClient(
            authToken,
            endpoints.cacheEndpoint(),
            itemDefaultTtlSeconds,
            telemetryOptional,
            requestTimeout);
  }

  public static SimpleCacheClientBuilder builder(String authToken, long itemDefaultTtlSeconds) {
    return new SimpleCacheClientBuilder(authToken, itemDefaultTtlSeconds);
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
   * @param ttlMinutes The key's time-to-live in minutes
   * @return The created key and its metadata
   */
  public CreateSigningKeyResponse createSigningKey(int ttlMinutes) {
    return scsControlClient.createSigningKey(ttlMinutes, scsDataClient.getEndpoint());
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
   * @param ttlSeconds Time to Live for the item in Cache. This ttl takes precedence over the TTL
   *     used when building a cache client {@link SimpleCacheClient#builder(String, long)}
   * @return Future containing the result of the set operation.
   */
  public CompletableFuture<CacheSetResponse> set(
      String cacheName, String key, ByteBuffer value, long ttlSeconds) {
    return scsDataClient.set(cacheName, key, value, ttlSeconds);
  }

  /**
   * Sets the value in the cache. If a value for this key is already present it will be replaced by
   * the new value.
   *
   * <p>The Time to Live (TTL) seconds defaults to the parameter used when building this Cache
   * client - {@link SimpleCacheClient#builder(String, long)}
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
   * @param ttlSeconds Time to Live for the item in Cache. This ttl takes precedence over the TTL
   *     used when building a cache client {@link SimpleCacheClient#builder(String, long)}
   * @return Future containing the result of the set operation.
   */
  public CompletableFuture<CacheSetResponse> set(
      String cacheName, byte[] key, byte[] value, long ttlSeconds) {
    return scsDataClient.set(cacheName, key, value, ttlSeconds);
  }

  /**
   * Sets the value in the cache. If a value for this key is already present it will be replaced by
   * the new value.
   *
   * <p>The Time to Live (TTL) seconds defaults to the parameter used when building this Cache
   * client - {@link SimpleCacheClient#builder(String, long)}
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
   * @param ttlSeconds Time to Live for the item in Cache. This ttl takes precedence over the TTL
   *     used when building a cache client {@link SimpleCacheClient#builder(String, long)}
   * @return Future containing the result of the set operation.
   */
  public CompletableFuture<CacheSetResponse> set(
      String cacheName, String key, String value, long ttlSeconds) {
    return scsDataClient.set(cacheName, key, value, ttlSeconds);
  }

  /**
   * Sets the value in the cache. If a value for this key is already present it will be replaced by
   * the new value.
   *
   * <p>The Time to Live (TTL) seconds defaults to the parameter used when building this Cache
   * client - {@link SimpleCacheClient#builder(String, long)}
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
   * Increments the value in the cache by an amount.
   *
   * @param cacheName Name of the cache to store the item in
   * @param field The field under which the value is to be added.
   * @param amount The amount by which the cache value is to be incremented.
   * @param ttlSeconds Time to Live for the item in Cache. This ttl takes precedence over the TTL
   *     used when building a cache client {@link SimpleCacheClient#builder(String, long)}
   * @return Future containing the result of the increment operation.
   */
  public CompletableFuture<CacheIncrementResponse> increment(
      String cacheName, String field, long amount, long ttlSeconds) {
    return scsDataClient.increment(cacheName, field, amount, ttlSeconds);
  }

  /**
   * Increments the value in the cache by an amount.
   *
   * @param cacheName Name of the cache to store the item in
   * @param field The field under which the value is to be added.
   * @param amount The amount by which the cache value is to be incremented.
   * @param ttlSeconds Time to Live for the item in Cache. This ttl takes precedence over the TTL
   *     used when building a cache client {@link SimpleCacheClient#builder(String, long)}
   * @return Future containing the result of the increment operation.
   */
  public CompletableFuture<CacheIncrementResponse> increment(
      String cacheName, byte[] field, long amount, long ttlSeconds) {
    return scsDataClient.increment(cacheName, field, amount, ttlSeconds);
  }

  @Override
  public void close() {
    scsControlClient.close();
    scsDataClient.close();
  }
}
