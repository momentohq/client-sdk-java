package momento.sdk;

import io.opentelemetry.api.OpenTelemetry;
import java.io.Closeable;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import momento.sdk.exceptions.AlreadyExistsException;
import momento.sdk.exceptions.BadRequestException;
import momento.sdk.exceptions.ClientSdkException;
import momento.sdk.exceptions.NotFoundException;
import momento.sdk.messages.*;

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
   * @throws momento.sdk.exceptions.PermissionDeniedException
   * @throws BadRequestException
   * @throws AlreadyExistsException
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
   * @throws NotFoundException
   * @throws momento.sdk.exceptions.InternalServerException
   * @throws ClientSdkException if the {@code cacheName} is null.
   */
  public DeleteCacheResponse deleteCache(String cacheName) {
    return scsControlClient.deleteCache(cacheName);
  }

  /**
   * Flushes the contents of the cache.
   *
   * @param cacheName The name of the cache to be flushed.
   * @return The result of the cache flush operation.
   * @throws momento.sdk.exceptions.PermissionDeniedException
   * @throws NotFoundException
   * @throws momento.sdk.exceptions.InternalServerException
   * @throws ClientSdkException if the {@code cacheName} is null.
   */
  public FlushCacheResponse flushCache(String cacheName) {
    return scsControlClient.flushCache(cacheName);
  }

  /**
   * Lists all caches for the provided auth token.
   *
   * <pre>{@code
   * Optional<String> nextPageToken = Optional.empty();
   * do {
   *     ListCachesResponse response = simpleCacheClient.listCaches(nextPageToken);
   *
   *     // Your code here to use the response
   *
   *     nextPageToken = response.nextPageToken();
   * } while(nextPageToken.isPresent());
   * }</pre>
   */
  public ListCachesResponse listCaches(Optional<String> nextToken) {
    return scsControlClient.listCaches(nextToken);
  }

  /**
   * Creates a new Momento signing key
   *
   * @param ttlMinutes The key's time-to-live in minutes
   * @return The created key and its metadata
   * @throws momento.sdk.exceptions.PermissionDeniedException
   * @throws NotFoundException
   * @throws momento.sdk.exceptions.InternalServerException
   * @throws ClientSdkException if the {@code ttlMinutes} is invalid.
   */
  public CreateSigningKeyResponse createSigningKey(int ttlMinutes) {
    return scsControlClient.createSigningKey(ttlMinutes, scsDataClient.getEndpoint());
  }

  /**
   * Revokes a Momento signing key, all tokens signed by which will be invalid
   *
   * @param keyId The id of the key to revoke
   * @return
   * @throws momento.sdk.exceptions.PermissionDeniedException
   * @throws NotFoundException
   * @throws momento.sdk.exceptions.InternalServerException
   * @throws ClientSdkException if the {@code keyId} is null.
   */
  public RevokeSigningKeyResponse revokeSigningKey(String keyId) {
    return scsControlClient.revokeSigningKey(keyId);
  }

  /**
   * Lists all Momento signing keys for the provided auth token.
   *
   * <pre>{@code
   * Optional<String> nextToken = Optional.empty();
   * do {
   *    ListSigningKeysResponse response = simpleCacheClient.listSigningKeys(nextToken);
   *
   *    // Your code here to use the response
   *
   *    nextToken = response.nextToken();
   * } while (nextToken.isPresent());
   * }</pre>
   *
   * @param nextToken Optional pagination token
   * @return A list of Momento signing keys along with a pagination token (if present)
   * @throws momento.sdk.exceptions.PermissionDeniedException
   * @throws NotFoundException
   * @throws momento.sdk.exceptions.InternalServerException
   */
  public ListSigningKeysResponse listSigningKeys(Optional<String> nextToken) {
    return scsControlClient.listSigningKeys(nextToken, scsDataClient.getEndpoint());
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
   * @throws momento.sdk.exceptions.PermissionDeniedException
   * @throws ClientSdkException if key is null
   * @throws NotFoundException
   * @throws momento.sdk.exceptions.InternalServerException
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
   * @throws momento.sdk.exceptions.PermissionDeniedException
   * @throws ClientSdkException if key is null
   * @throws NotFoundException
   * @throws momento.sdk.exceptions.InternalServerException
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
   * @throws momento.sdk.exceptions.PermissionDeniedException
   * @throws ClientSdkException if key or value is null or ttlSeconds is less than or equal to zero
   * @throws NotFoundException
   * @throws momento.sdk.exceptions.InternalServerException
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
   * @throws momento.sdk.exceptions.PermissionDeniedException
   * @throws ClientSdkException if key or value is null
   * @throws NotFoundException
   * @throws momento.sdk.exceptions.InternalServerException
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
   * @throws momento.sdk.exceptions.PermissionDeniedException
   * @throws ClientSdkException if key or value is null or ttlSeconds is less than or equal to zero
   * @throws NotFoundException
   * @throws momento.sdk.exceptions.InternalServerException
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
   * @throws momento.sdk.exceptions.PermissionDeniedException
   * @throws ClientSdkException if key or value is null
   * @throws NotFoundException
   * @throws momento.sdk.exceptions.InternalServerException
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
   * @throws momento.sdk.exceptions.PermissionDeniedException
   * @throws ClientSdkException if key or value is null or ttlSeconds is less than or equal to zero
   * @throws NotFoundException
   * @throws momento.sdk.exceptions.InternalServerException
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
   * @throws momento.sdk.exceptions.PermissionDeniedException
   * @throws ClientSdkException if key or value is null
   * @throws NotFoundException
   * @throws momento.sdk.exceptions.InternalServerException
   */
  public CompletableFuture<CacheSetResponse> set(String cacheName, String key, String value) {
    return scsDataClient.set(cacheName, key, value);
  }

  @Override
  public void close() {
    scsControlClient.close();
    scsDataClient.close();
  }
}
