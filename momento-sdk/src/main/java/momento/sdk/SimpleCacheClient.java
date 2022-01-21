package momento.sdk;

import io.opentelemetry.api.OpenTelemetry;
import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import momento.sdk.exceptions.CacheAlreadyExistsException;
import momento.sdk.exceptions.CacheNotFoundException;
import momento.sdk.exceptions.ClientSdkException;
import momento.sdk.messages.CacheGetResponse;
import momento.sdk.messages.CacheSetResponse;
import momento.sdk.messages.CreateCacheResponse;
import momento.sdk.messages.DeleteCacheResponse;
import momento.sdk.messages.ListCachesRequest;
import momento.sdk.messages.ListCachesResponse;

/** Client to perform operations against the Simple Cache Service */
public final class SimpleCacheClient implements Closeable {

  private final ScsControlClient scsControlClient;
  private final ScsDataClient scsDataClient;

  SimpleCacheClient(
      String authToken, int itemDefaultTtlSeconds, Optional<OpenTelemetry> telemetryOptional) {
    MomentoEndpointsResolver.MomentoEndpoints endpoints =
        MomentoEndpointsResolver.resolve(authToken, Optional.empty());
    this.scsControlClient = new ScsControlClient(authToken, endpoints.controlEndpoint());
    this.scsDataClient =
        new ScsDataClient(
            authToken, endpoints.cacheEndpoint(), itemDefaultTtlSeconds, telemetryOptional);
  }

  public static SimpleCacheClientBuilder builder(String authToken, int itemDefaultTtlSeconds) {
    return new SimpleCacheClientBuilder(authToken, itemDefaultTtlSeconds);
  }

  /**
   * Creates a cache with provided name
   *
   * @param cacheName Name of the cache to be created.
   * @return The result of the create cache operation
   * @throws momento.sdk.exceptions.PermissionDeniedException
   * @throws momento.sdk.exceptions.InvalidArgumentException
   * @throws CacheAlreadyExistsException
   * @throws momento.sdk.exceptions.InternalServerException
   * @throws ClientSdkException when cacheName is null
   */
  public CreateCacheResponse createCache(String cacheName) {
    return scsControlClient.createCache(cacheName);
  }

  /**
   * Deletes a cache
   *
   * @param cacheName The name of the cache to be deleted.
   * @return The result of the cache deletion operation.
   * @throws momento.sdk.exceptions.PermissionDeniedException
   * @throws CacheNotFoundException
   * @throws momento.sdk.exceptions.InternalServerException
   * @throws ClientSdkException if the {@code cacheName} is null.
   */
  public DeleteCacheResponse deleteCache(String cacheName) {
    return scsControlClient.deleteCache(cacheName);
  }

  /** Lists all caches for the provided auth token. */
  public ListCachesResponse listCaches(ListCachesRequest request) {
    return scsControlClient.listCaches(request);
  }

  /**
   * Get the cache value stored for the given key.
   *
   * @param cacheName Name of the cache to get value from
   * @param key The key to get
   * @return {@link CacheGetResponse} containing the status of the get operation and the associated
   *     value data.
   * @throws momento.sdk.exceptions.PermissionDeniedException
   * @throws ClientSdkException if key is null
   * @throws momento.sdk.exceptions.CacheNotFoundException
   * @throws momento.sdk.exceptions.InternalServerException
   */
  public CacheGetResponse get(String cacheName, String key) {
    return scsDataClient.get(cacheName, key);
  }

  /**
   * Get the cache value stored for the given key.
   *
   * @param cacheName Name of the cache to get value from
   * @param key The key to get
   * @return {@link CacheGetResponse} containing the status of the get operation and the associated
   *     value data.
   * @throws momento.sdk.exceptions.PermissionDeniedException
   * @throws ClientSdkException if key is null
   * @throws momento.sdk.exceptions.CacheNotFoundException
   * @throws momento.sdk.exceptions.InternalServerException
   */
  public CacheGetResponse get(String cacheName, byte[] key) {
    return scsDataClient.get(cacheName, key);
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
   *     used when building a cache client {@link SimpleCacheClient#builder(String, int)}
   * @return Result of the set operation.
   * @throws momento.sdk.exceptions.PermissionDeniedException
   * @throws ClientSdkException if key, value is null or if ttlSeconds is less than or equal to zero
   * @throws momento.sdk.exceptions.CacheNotFoundException
   * @throws momento.sdk.exceptions.InternalServerException
   */
  public CacheSetResponse set(String cacheName, String key, ByteBuffer value, int ttlSeconds) {
    return scsDataClient.set(cacheName, key, value, ttlSeconds);
  }

  /**
   * Sets the value in the cache. If a value for this key is already present it will be replaced by
   * the new value.
   *
   * <p>The Time to Live (TTL) seconds defaults to the parameter used when building this Cache
   * client - {@link SimpleCacheClient#builder(String, int)}
   *
   * @param cacheName Name of the cache to store the item in
   * @param key The key under which the value is to be added.
   * @param value The value to be stored.
   * @return Result of the set operation.
   * @throws momento.sdk.exceptions.PermissionDeniedException
   * @throws ClientSdkException if key or value is null
   * @throws momento.sdk.exceptions.CacheNotFoundException
   * @throws momento.sdk.exceptions.InternalServerException
   */
  public CacheSetResponse set(String cacheName, String key, ByteBuffer value) {
    return scsDataClient.set(cacheName, key, value);
  }

  /**
   * Sets the value in cache with a given Time To Live (TTL) seconds.
   *
   * <p>If a value for this key is already present it will be replaced by the new value.
   *
   * @param key The key under which the value is to be added.
   * @param value The value to be stored.
   * @param ttlSeconds Time to Live for the item in Cache. This ttl takes precedence over the TTL
   *     used when building a cache client {@link SimpleCacheClient#builder(String, int)}
   * @return Result of the set operation.
   * @throws momento.sdk.exceptions.PermissionDeniedException
   * @throws ClientSdkException if key or value is null or ttlSeconds is less than or equal to zero
   * @throws momento.sdk.exceptions.CacheNotFoundException
   * @throws momento.sdk.exceptions.InternalServerException
   */
  public CacheSetResponse set(String cacheName, String key, String value, int ttlSeconds) {
    return scsDataClient.set(cacheName, key, value, ttlSeconds);
  }

  /**
   * Sets the value in the cache. If a value for this key is already present it will be replaced by
   * the new value.
   *
   * <p>The Time to Live (TTL) seconds defaults to the parameter used when building this Cache
   * client - {@link SimpleCacheClient#builder(String, int)}
   *
   * @param cacheName Name of the cache to store the item in
   * @param key The key under which the value is to be added.
   * @param value The value to be stored.
   * @return Result of the set operation.
   * @throws momento.sdk.exceptions.PermissionDeniedException
   * @throws ClientSdkException if key or value is null
   * @throws momento.sdk.exceptions.CacheNotFoundException
   * @throws momento.sdk.exceptions.InternalServerException
   */
  public CacheSetResponse set(String cacheName, String key, String value) {
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
   *     used when building a cache client {@link SimpleCacheClient#builder(String, int)}
   * @return Result of the set operation.
   * @throws momento.sdk.exceptions.PermissionDeniedException
   * @throws ClientSdkException if key or value is null or ttlSeconds is less than or equal to zero
   * @throws momento.sdk.exceptions.CacheNotFoundException
   * @throws momento.sdk.exceptions.InternalServerException
   */
  public CacheSetResponse set(String cacheName, byte[] key, byte[] value, int ttlSeconds) {
    return scsDataClient.set(cacheName, key, value, ttlSeconds);
  }

  /**
   * Sets the value in the cache. If a value for this key is already present it will be replaced by
   * the new value.
   *
   * <p>The Time to Live (TTL) seconds defaults to the parameter used when building this Cache
   * client - {@link SimpleCacheClient#builder(String, int)}
   *
   * @param cacheName Name of the cache to store the item in
   * @param key The key under which the value is to be added.
   * @param value The value to be stored.
   * @return Result of the set operation.
   * @throws momento.sdk.exceptions.PermissionDeniedException
   * @throws ClientSdkException if key or value is null
   * @throws momento.sdk.exceptions.CacheNotFoundException
   * @throws momento.sdk.exceptions.InternalServerException
   */
  public CacheSetResponse set(String cacheName, byte[] key, byte[] value) {
    return scsDataClient.set(cacheName, key, value);
  }

  /**
   * Get the cache value stored for the given key.
   *
   * @param cacheName Name of the cache to get the item from
   * @param key The key to get
   * @return Future with {@link CacheGetResponse} containing the status of the get operation and the
   *     associated value data.
   * @throws momento.sdk.exceptions.PermissionDeniedException
   * @throws ClientSdkException if key is null
   * @throws momento.sdk.exceptions.CacheNotFoundException
   * @throws momento.sdk.exceptions.InternalServerException
   */
  public CompletableFuture<CacheGetResponse> getAsync(String cacheName, byte[] key) {
    return scsDataClient.getAsync(cacheName, key);
  }

  /**
   * Get the cache value stored for the given key.
   *
   * @param cacheName Name of the cache to get the item from
   * @param key The key to get
   * @return Future with {@link CacheGetResponse} containing the status of the get operation and the
   *     associated value data.
   * @throws momento.sdk.exceptions.PermissionDeniedException
   * @throws ClientSdkException if key is null
   * @throws momento.sdk.exceptions.CacheNotFoundException
   * @throws momento.sdk.exceptions.InternalServerException
   */
  public CompletableFuture<CacheGetResponse> getAsync(String cacheName, String key) {
    return scsDataClient.getAsync(cacheName, key);
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
   *     used when building a cache client {@link SimpleCacheClient#builder(String, int)}
   * @return Future containing the result of the set operation.
   * @throws momento.sdk.exceptions.PermissionDeniedException
   * @throws ClientSdkException if key or value is null or ttlSeconds is less than or equal to zero
   * @throws momento.sdk.exceptions.CacheNotFoundException
   * @throws momento.sdk.exceptions.InternalServerException
   */
  public CompletableFuture<CacheSetResponse> setAsync(
      String cacheName, String key, ByteBuffer value, int ttlSeconds) {
    return scsDataClient.setAsync(cacheName, key, value, ttlSeconds);
  }

  /**
   * Sets the value in the cache. If a value for this key is already present it will be replaced by
   * the new value.
   *
   * <p>The Time to Live (TTL) seconds defaults to the parameter used when building this Cache
   * client - {@link SimpleCacheClient#builder(String, int)}
   *
   * @param cacheName Name of the cache to store the item in
   * @param key The key under which the value is to be added.
   * @param value The value to be stored.
   * @return Future containing the result of the set operation.
   * @throws momento.sdk.exceptions.PermissionDeniedException
   * @throws ClientSdkException if key or value is null
   * @throws momento.sdk.exceptions.CacheNotFoundException
   * @throws momento.sdk.exceptions.InternalServerException
   */
  public CompletableFuture<CacheSetResponse> setAsync(
      String cacheName, String key, ByteBuffer value) {
    return scsDataClient.setAsync(cacheName, key, value);
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
   *     used when building a cache client {@link SimpleCacheClient#builder(String, int)}
   * @return Future containing the result of the set operation.
   * @throws momento.sdk.exceptions.PermissionDeniedException
   * @throws ClientSdkException if key or value is null or ttlSeconds is less than or equal to zero
   * @throws momento.sdk.exceptions.CacheNotFoundException
   * @throws momento.sdk.exceptions.InternalServerException
   */
  public CompletableFuture<CacheSetResponse> setAsync(
      String cacheName, byte[] key, byte[] value, int ttlSeconds) {
    return scsDataClient.setAsync(cacheName, key, value, ttlSeconds);
  }

  /**
   * Sets the value in the cache. If a value for this key is already present it will be replaced by
   * the new value.
   *
   * <p>The Time to Live (TTL) seconds defaults to the parameter used when building this Cache
   * client - {@link SimpleCacheClient#builder(String, int)}
   *
   * @param cacheName Name of the cache to store the item in
   * @param key The key under which the value is to be added.
   * @param value The value to be stored.
   * @return Future containing the result of the set operation.
   * @throws momento.sdk.exceptions.PermissionDeniedException
   * @throws ClientSdkException if key or value is null
   * @throws momento.sdk.exceptions.CacheNotFoundException
   * @throws momento.sdk.exceptions.InternalServerException
   */
  public CompletableFuture<CacheSetResponse> setAsync(String cacheName, byte[] key, byte[] value) {
    return scsDataClient.setAsync(cacheName, key, value);
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
   *     used when building a cache client {@link SimpleCacheClient#builder(String, int)}
   * @return Future containing the result of the set operation.
   * @throws momento.sdk.exceptions.PermissionDeniedException
   * @throws ClientSdkException if key or value is null or ttlSeconds is less than or equal to zero
   * @throws momento.sdk.exceptions.CacheNotFoundException
   * @throws momento.sdk.exceptions.InternalServerException
   */
  public CompletableFuture<CacheSetResponse> setAsync(
      String cacheName, String key, String value, int ttlSeconds) {
    return scsDataClient.setAsync(cacheName, key, value, ttlSeconds);
  }

  /**
   * Sets the value in the cache. If a value for this key is already present it will be replaced by
   * the new value.
   *
   * <p>The Time to Live (TTL) seconds defaults to the parameter used when building this Cache
   * client - {@link SimpleCacheClient#builder(String, int)}
   *
   * @param cacheName Name of the cache to store the item in
   * @param key The key under which the value is to be added.
   * @param value The value to be stored.
   * @return Future containing the result of the set operation.
   * @throws momento.sdk.exceptions.PermissionDeniedException
   * @throws ClientSdkException if key or value is null
   * @throws momento.sdk.exceptions.CacheNotFoundException
   * @throws momento.sdk.exceptions.InternalServerException
   */
  public CompletableFuture<CacheSetResponse> setAsync(String cacheName, String key, String value) {
    return scsDataClient.setAsync(cacheName, key, value);
  }

  @Override
  public void close() {
    scsControlClient.close();
    scsDataClient.close();
  }
}