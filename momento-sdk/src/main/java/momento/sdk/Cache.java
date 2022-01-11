package momento.sdk;

import com.google.protobuf.ByteString;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import momento.sdk.exceptions.CacheServiceExceptionMapper;
import momento.sdk.exceptions.ClientSdkException;
import momento.sdk.exceptions.SdkException;
import momento.sdk.messages.CacheGetResponse;
import momento.sdk.messages.CacheSetResponse;

/** Entity that represents a named Momento Cache.*/
public final class Cache {

  private final String cacheName;
  private final int itemDefaultTtlSeconds;
  private final ScsGrpcClient scsGrpcClient;

  Cache(String cacheName, int itemDefaultTtlSeconds, ScsGrpcClient scsGrpcClient) {
    this.cacheName = cacheName;
    this.itemDefaultTtlSeconds = itemDefaultTtlSeconds;
    this.scsGrpcClient = scsGrpcClient;
  }

  // Runs a get using the provided cache name and the auth token.
  //
  // An alternate approach would be to make this call during construction itself. That however, may
  // cause Cache object construction to fail and leave behind open grpc channels. Eventually those
  // would be garbage collected.
  //
  // The separation between opening a grpc channel vs performing operations against the Momento
  // Cache construct allows SDK builders a better control to manage objects. This is particularly
  // useful for getOrCreateCache calls. Doing a get first is desirable as our data plane can take
  // more load as compared to the control plane. However, if a cache doesn't exist the constructor
  // may end up failing and then upon cache creation using the control plane a new server connection
  // would have to establish. This paradigm is a minor but desirable optimization to prevent opening
  // multiple channels and incurring the cost.
  Cache connect() {
    this.testConnection();
    return this;
  }

  private void testConnection() {
    try {
      this.get(UUID.randomUUID().toString());
    } catch (Exception e) {
      throw CacheServiceExceptionMapper.convert(e);
    }
  }

  /**
   * Get the cache value stored for the given key.
   *
   * @param key The key to get
   * @return {@link CacheGetResponse} containing the status of the get operation and the associated
   *     value data.
   * @throws momento.sdk.exceptions.PermissionDeniedException
   * @throws ClientSdkException if key is null
   * @throws momento.sdk.exceptions.CacheNotFoundException
   * @throws momento.sdk.exceptions.InternalServerException
   * @see Cache#getAsync(String)
   */
  public CacheGetResponse get(String key) {
    ensureValidKey(key);
    return sendBlockingGet(convert(key));
  }

  /**
   * Get the cache value stored for the given key.
   *
   * @param key The key to get
   * @return {@link CacheGetResponse} containing the status of the get operation and the associated
   *     value data.
   * @throws momento.sdk.exceptions.PermissionDeniedException
   * @throws ClientSdkException if key is null
   * @throws momento.sdk.exceptions.CacheNotFoundException
   * @throws momento.sdk.exceptions.InternalServerException
   * @see Cache#getAsync(byte[])
   */
  public CacheGetResponse get(byte[] key) {
    ensureValidKey(key);
    return sendBlockingGet(convert(key));
  }

  /**
   * Sets the value in cache with a given Time To Live (TTL) seconds.
   *
   * <p>If a value for this key is already present it will be replaced by the new value.
   *
   * @param key The key under which the value is to be added.
   * @param value The value to be stored.
   * @param ttlSeconds Time to Live for the item in Cache. This ttl takes precedence over the TTL
   *     used when building a cache client {@link Momento#cacheBuilder(String, int)}
   * @return Result of the set operation.
   * @throws momento.sdk.exceptions.PermissionDeniedException
   * @throws ClientSdkException if key, value is null or if ttlSeconds is less than or equal to zero
   * @throws momento.sdk.exceptions.CacheNotFoundException
   * @throws momento.sdk.exceptions.InternalServerException
   * @see Cache#set(String, ByteBuffer)
   * @see Cache#setAsync(String, ByteBuffer, int)
   */
  public CacheSetResponse set(String key, ByteBuffer value, int ttlSeconds) {
    ensureValid(key, value, ttlSeconds);
    return sendBlockingSet(convert(key), convert(value), ttlSeconds);
  }

  /**
   * Sets the value in the cache. If a value for this key is already present it will be replaced by
   * the new value.
   *
   * <p>The Time to Live (TTL) seconds defaults to the parameter used when building this Cache
   * client - {@link Momento#cacheBuilder(String, int)}
   *
   * @param key The key under which the value is to be added.
   * @param value The value to be stored.
   * @return Result of the set operation.
   * @throws momento.sdk.exceptions.PermissionDeniedException
   * @throws ClientSdkException if key or value is null
   * @throws momento.sdk.exceptions.CacheNotFoundException
   * @throws momento.sdk.exceptions.InternalServerException
   * @see Cache#set(String, ByteBuffer, int)
   * @see Cache#setAsync(String, ByteBuffer)
   */
  public CacheSetResponse set(String key, ByteBuffer value) {
    return set(key, value, itemDefaultTtlSeconds);
  }

  /**
   * Sets the value in cache with a given Time To Live (TTL) seconds.
   *
   * <p>If a value for this key is already present it will be replaced by the new value.
   *
   * @param key The key under which the value is to be added.
   * @param value The value to be stored.
   * @param ttlSeconds Time to Live for the item in Cache. This ttl takes precedence over the TTL
   *     used when building a cache client {@link Momento#cacheBuilder(String, int)}
   * @return Result of the set operation.
   * @throws momento.sdk.exceptions.PermissionDeniedException
   * @throws ClientSdkException if key or value is null or ttlSeconds is less than or equal to zero
   * @throws momento.sdk.exceptions.CacheNotFoundException
   * @throws momento.sdk.exceptions.InternalServerException
   * @see Cache#set(String, String)
   * @see Cache#setAsync(String, String, int)
   */
  public CacheSetResponse set(String key, String value, int ttlSeconds) {
    ensureValid(key, value, ttlSeconds);
    return sendBlockingSet(convert(key), convert(value), ttlSeconds);
  }

  /**
   * Sets the value in the cache. If a value for this key is already present it will be replaced by
   * the new value.
   *
   * <p>The Time to Live (TTL) seconds defaults to the parameter used when building this Cache
   * client - {@link Momento#cacheBuilder(String, int)}
   *
   * @param key The key under which the value is to be added.
   * @param value The value to be stored.
   * @return Result of the set operation.
   * @throws momento.sdk.exceptions.PermissionDeniedException
   * @throws ClientSdkException if key or value is null
   * @throws momento.sdk.exceptions.CacheNotFoundException
   * @throws momento.sdk.exceptions.InternalServerException
   * @see Cache#set(String, String, int)
   * @see Cache#setAsync(String, String)
   */
  public CacheSetResponse set(String key, String value) {
    return set(key, value, itemDefaultTtlSeconds);
  }

  /**
   * Sets the value in cache with a given Time To Live (TTL) seconds.
   *
   * <p>If a value for this key is already present it will be replaced by the new value.
   *
   * @param key The key under which the value is to be added.
   * @param value The value to be stored.
   * @param ttlSeconds Time to Live for the item in Cache. This ttl takes precedence over the TTL
   *     used when building a cache client {@link Momento#cacheBuilder(String, int)}
   * @return Result of the set operation.
   * @throws momento.sdk.exceptions.PermissionDeniedException
   * @throws ClientSdkException if key or value is null or ttlSeconds is less than or equal to zero
   * @throws momento.sdk.exceptions.CacheNotFoundException
   * @throws momento.sdk.exceptions.InternalServerException
   * @see Cache#set(byte[], byte[])
   * @see Cache#setAsync(byte[], byte[], int)
   */
  public CacheSetResponse set(byte[] key, byte[] value, int ttlSeconds) {
    ensureValid(key, value, ttlSeconds);
    return sendBlockingSet(convert(key), convert(value), ttlSeconds);
  }

  /**
   * Sets the value in the cache. If a value for this key is already present it will be replaced by
   * the new value.
   *
   * <p>The Time to Live (TTL) seconds defaults to the parameter used when building this Cache
   * client - {@link Momento#cacheBuilder(String, int)}
   *
   * @param key The key under which the value is to be added.
   * @param value The value to be stored.
   * @return Result of the set operation.
   * @throws momento.sdk.exceptions.PermissionDeniedException
   * @throws ClientSdkException if key or value is null
   * @throws momento.sdk.exceptions.CacheNotFoundException
   * @throws momento.sdk.exceptions.InternalServerException
   * @see Cache#set(byte[], byte[], int)
   * @see Cache#setAsync(byte[], byte[])
   */
  public CacheSetResponse set(byte[] key, byte[] value) {
    return set(key, value, itemDefaultTtlSeconds);
  }

  private CacheGetResponse sendBlockingGet(ByteString key) {
    try {
      return scsGrpcClient.sendGet(cacheName, key).get();
    } catch (Throwable t) {
      throw handleExceptionally(t);
    }
  }

  private CacheSetResponse sendBlockingSet(ByteString key, ByteString value, int itemTtlSeconds) {
    try {
      return scsGrpcClient.sendSet(cacheName, key, value, itemTtlSeconds).get();
    } catch (Throwable t) {
      throw handleExceptionally(t);
    }
  }

  /**
   * Get the cache value stored for the given key.
   *
   * @param key The key to get
   * @return Future with {@link CacheGetResponse} containing the status of the get operation and the
   *     associated value data.
   * @throws momento.sdk.exceptions.PermissionDeniedException
   * @throws ClientSdkException if key is null
   * @throws momento.sdk.exceptions.CacheNotFoundException
   * @throws momento.sdk.exceptions.InternalServerException
   * @see Cache#get(byte[])
   */
  public CompletableFuture<CacheGetResponse> getAsync(byte[] key) {
    ensureValidKey(key);
    return scsGrpcClient.sendGet(cacheName, convert(key));
  }

  /**
   * Get the cache value stored for the given key.
   *
   * @param key The key to get
   * @return Future with {@link CacheGetResponse} containing the status of the get operation and the
   *     associated value data.
   * @throws momento.sdk.exceptions.PermissionDeniedException
   * @throws ClientSdkException if key is null
   * @throws momento.sdk.exceptions.CacheNotFoundException
   * @throws momento.sdk.exceptions.InternalServerException
   * @see Cache#get(String)
   */
  public CompletableFuture<CacheGetResponse> getAsync(String key) {
    ensureValidKey(key);
    return scsGrpcClient.sendGet(cacheName, convert(key));
  }

  /**
   * Sets the value in cache with a given Time To Live (TTL) seconds.
   *
   * <p>If a value for this key is already present it will be replaced by the new value.
   *
   * @param key The key under which the value is to be added.
   * @param value The value to be stored.
   * @param ttlSeconds Time to Live for the item in Cache. This ttl takes precedence over the TTL
   *     used when building a cache client {@link Momento#cacheBuilder(String, int)}
   * @return Future containing the result of the set operation.
   * @throws momento.sdk.exceptions.PermissionDeniedException
   * @throws ClientSdkException if key or value is null or ttlSeconds is less than or equal to zero
   * @throws momento.sdk.exceptions.CacheNotFoundException
   * @throws momento.sdk.exceptions.InternalServerException
   * @see Cache#setAsync(String, ByteBuffer)
   * @see Cache#set(String, ByteBuffer, int)
   */
  public CompletableFuture<CacheSetResponse> setAsync(
      String key, ByteBuffer value, int ttlSeconds) {
    ensureValid(key, value, ttlSeconds);
    return scsGrpcClient.sendSet(cacheName, convert(key), convert(value), ttlSeconds);
  }

  /**
   * Sets the value in the cache. If a value for this key is already present it will be replaced by
   * the new value.
   *
   * <p>The Time to Live (TTL) seconds defaults to the parameter used when building this Cache
   * client - {@link Momento#cacheBuilder(String, int)}
   *
   * @param key The key under which the value is to be added.
   * @param value The value to be stored.
   * @return Future containing the result of the set operation.
   * @throws momento.sdk.exceptions.PermissionDeniedException
   * @throws ClientSdkException if key or value is null
   * @throws momento.sdk.exceptions.CacheNotFoundException
   * @throws momento.sdk.exceptions.InternalServerException
   * @see Cache#set(String, ByteBuffer)
   * @see Cache#setAsync(String, ByteBuffer, int)
   */
  public CompletableFuture<CacheSetResponse> setAsync(String key, ByteBuffer value) {
    return setAsync(key, value, itemDefaultTtlSeconds);
  }

  /**
   * Sets the value in cache with a given Time To Live (TTL) seconds.
   *
   * <p>If a value for this key is already present it will be replaced by the new value.
   *
   * @param key The key under which the value is to be added.
   * @param value The value to be stored.
   * @param ttlSeconds Time to Live for the item in Cache. This ttl takes precedence over the TTL
   *     used when building a cache client {@link Momento#cacheBuilder(String, int)}
   * @return Future containing the result of the set operation.
   * @throws momento.sdk.exceptions.PermissionDeniedException
   * @throws ClientSdkException if key or value is null or ttlSeconds is less than or equal to zero
   * @throws momento.sdk.exceptions.CacheNotFoundException
   * @throws momento.sdk.exceptions.InternalServerException
   * @see Cache#setAsync(byte[], byte[])
   * @see Cache#set(byte[], byte[], int)
   */
  public CompletableFuture<CacheSetResponse> setAsync(byte[] key, byte[] value, int ttlSeconds) {
    ensureValid(key, value, ttlSeconds);
    return scsGrpcClient.sendSet(cacheName, convert(key), convert(value), ttlSeconds);
  }

  /**
   * Sets the value in the cache. If a value for this key is already present it will be replaced by
   * the new value.
   *
   * <p>The Time to Live (TTL) seconds defaults to the parameter used when building this Cache
   * client - {@link Momento#cacheBuilder(String, int)}
   *
   * @param key The key under which the value is to be added.
   * @param value The value to be stored.
   * @return Future containing the result of the set operation.
   * @throws momento.sdk.exceptions.PermissionDeniedException
   * @throws ClientSdkException if key or value is null
   * @throws momento.sdk.exceptions.CacheNotFoundException
   * @throws momento.sdk.exceptions.InternalServerException
   * @see Cache#setAsync(byte[], byte[], int)
   * @see Cache#set(byte[], byte[])
   */
  public CompletableFuture<CacheSetResponse> setAsync(byte[] key, byte[] value) {
    return setAsync(key, value, itemDefaultTtlSeconds);
  }

  /**
   * Sets the value in cache with a given Time To Live (TTL) seconds.
   *
   * <p>If a value for this key is already present it will be replaced by the new value.
   *
   * @param key The key under which the value is to be added.
   * @param value The value to be stored.
   * @param ttlSeconds Time to Live for the item in Cache. This ttl takes precedence over the TTL
   *     used when building a cache client {@link Momento#cacheBuilder(String, int)}
   * @return Future containing the result of the set operation.
   * @throws momento.sdk.exceptions.PermissionDeniedException
   * @throws ClientSdkException if key or value is null or ttlSeconds is less than or equal to zero
   * @throws momento.sdk.exceptions.CacheNotFoundException
   * @throws momento.sdk.exceptions.InternalServerException
   * @see Cache#setAsync(String, String)
   * @see Cache#set(String, String, int)
   */
  public CompletableFuture<CacheSetResponse> setAsync(String key, String value, int ttlSeconds) {
    ensureValid(key, value, ttlSeconds);
    return scsGrpcClient.sendSet(cacheName, convert(key), convert(value), ttlSeconds);
  }

  /**
   * Sets the value in the cache. If a value for this key is already present it will be replaced by
   * the new value.
   *
   * <p>The Time to Live (TTL) seconds defaults to the parameter used when building this Cache
   * client - {@link Momento#cacheBuilder(String, int)}
   *
   * @param key The key under which the value is to be added.
   * @param value The value to be stored.
   * @return Future containing the result of the set operation.
   * @throws momento.sdk.exceptions.PermissionDeniedException
   * @throws ClientSdkException if key or value is null
   * @throws momento.sdk.exceptions.CacheNotFoundException
   * @throws momento.sdk.exceptions.InternalServerException
   * @see Cache#setAsync(String, String, int)
   * @see Cache#set(String, String)
   */
  public CompletableFuture<CacheSetResponse> setAsync(String key, String value) {
    return setAsync(key, value, itemDefaultTtlSeconds);
  }

  private static void ensureValid(Object key, Object value, int ttlSeconds) {

    ensureValidKey(key);

    if (value == null) {
      throw new ClientSdkException("A non-null value is required.");
    }

    if (ttlSeconds <= 0) {
      throw new ClientSdkException("Item's time to live in Cache must be a positive integer.");
    }
  }

  private static void ensureValidKey(Object key) {
    if (key == null) {
      throw new ClientSdkException("A non-null Key is required.");
    }
  }

  private ByteString convert(String stringToEncode) {
    return ByteString.copyFromUtf8(stringToEncode);
  }

  private ByteString convert(byte[] bytes) {
    return ByteString.copyFrom(bytes);
  }

  private ByteString convert(ByteBuffer byteBuffer) {
    return ByteString.copyFrom(byteBuffer);
  }

  private static SdkException handleExceptionally(Throwable t) {
    if (t instanceof ExecutionException) {
      return CacheServiceExceptionMapper.convert(t.getCause());
    }
    return CacheServiceExceptionMapper.convert(t);
  }
}
