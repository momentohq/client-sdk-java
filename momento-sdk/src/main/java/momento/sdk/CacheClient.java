package momento.sdk;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import momento.sdk.auth.CredentialProvider;
import momento.sdk.config.Configuration;
import momento.sdk.messages.CacheDeleteResponse;
import momento.sdk.messages.CacheDictionaryFetchResponse;
import momento.sdk.messages.CacheDictionaryGetFieldResponse;
import momento.sdk.messages.CacheDictionaryGetFieldsResponse;
import momento.sdk.messages.CacheDictionaryIncrementResponse;
import momento.sdk.messages.CacheDictionaryRemoveFieldResponse;
import momento.sdk.messages.CacheDictionaryRemoveFieldsResponse;
import momento.sdk.messages.CacheDictionarySetFieldResponse;
import momento.sdk.messages.CacheDictionarySetFieldsResponse;
import momento.sdk.messages.CacheGetResponse;
import momento.sdk.messages.CacheIncrementResponse;
import momento.sdk.messages.CacheListConcatenateBackResponse;
import momento.sdk.messages.CacheListConcatenateFrontResponse;
import momento.sdk.messages.CacheListFetchResponse;
import momento.sdk.messages.CacheListLengthResponse;
import momento.sdk.messages.CacheListPopBackResponse;
import momento.sdk.messages.CacheListPopFrontResponse;
import momento.sdk.messages.CacheListPushBackResponse;
import momento.sdk.messages.CacheListPushFrontResponse;
import momento.sdk.messages.CacheListRemoveValueResponse;
import momento.sdk.messages.CacheListRetainResponse;
import momento.sdk.messages.CacheSetAddElementResponse;
import momento.sdk.messages.CacheSetAddElementsResponse;
import momento.sdk.messages.CacheSetFetchResponse;
import momento.sdk.messages.CacheSetIfNotExistsResponse;
import momento.sdk.messages.CacheSetRemoveElementResponse;
import momento.sdk.messages.CacheSetRemoveElementsResponse;
import momento.sdk.messages.CacheSetResponse;
import momento.sdk.messages.CacheSortedSetFetchResponse;
import momento.sdk.messages.CacheSortedSetPutElementResponse;
import momento.sdk.messages.CacheSortedSetPutElementsResponse;
import momento.sdk.messages.CreateCacheResponse;
import momento.sdk.messages.CreateSigningKeyResponse;
import momento.sdk.messages.DeleteCacheResponse;
import momento.sdk.messages.FlushCacheResponse;
import momento.sdk.messages.ListCachesResponse;
import momento.sdk.messages.ListSigningKeysResponse;
import momento.sdk.messages.RevokeSigningKeyResponse;
import momento.sdk.requests.CollectionTtl;

/** Client to perform operations against the Momento Cache Service */
public final class CacheClient implements Closeable {

  private final ScsControlClient scsControlClient;
  private final ScsDataClient scsDataClient;

  CacheClient(
      @Nonnull CredentialProvider credentialProvider,
      @Nonnull Configuration configuration,
      @Nonnull Duration itemDefaultTtl) {
    this.scsControlClient = new ScsControlClient(credentialProvider);
    this.scsDataClient = new ScsDataClient(credentialProvider, configuration, itemDefaultTtl);
  }

  public static CacheClientBuilder builder(
      CredentialProvider credentialProvider, Configuration configuration, Duration itemDefaultTtl) {
    return new CacheClientBuilder(credentialProvider, configuration, itemDefaultTtl);
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
    return scsControlClient.createSigningKey(ttl);
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
    return scsControlClient.listSigningKeys();
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
   *     building a cache client {@link CacheClient#builder(CredentialProvider, Configuration,
   *     Duration)}
   * @return Future containing the result of the set operation.
   */
  public CompletableFuture<CacheSetResponse> set(
      String cacheName, String key, ByteBuffer value, @Nullable Duration ttl) {
    return scsDataClient.set(cacheName, key, value, ttl);
  }

  /**
   * Sets the value in the cache. If a value for this key is already present it will be replaced by
   * the new value.
   *
   * <p>The Time to Live (TTL) seconds defaults to the parameter used when building this Cache
   * client - {@link CacheClient#builder(CredentialProvider, Configuration, Duration)}
   *
   * @param cacheName Name of the cache to store the item in
   * @param key The key under which the value is to be added.
   * @param value The value to be stored.
   * @return Future containing the result of the set operation.
   */
  public CompletableFuture<CacheSetResponse> set(String cacheName, String key, ByteBuffer value) {
    return scsDataClient.set(cacheName, key, value, null);
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
   *     building a cache client {@link CacheClient#builder(CredentialProvider, Configuration,
   *     Duration)}
   * @return Future containing the result of the set operation.
   */
  public CompletableFuture<CacheSetResponse> set(
      String cacheName, byte[] key, byte[] value, @Nullable Duration ttl) {
    return scsDataClient.set(cacheName, key, value, ttl);
  }

  /**
   * Sets the value in the cache. If a value for this key is already present it will be replaced by
   * the new value.
   *
   * <p>The Time to Live (TTL) seconds defaults to the parameter used when building this Cache
   * client - {@link CacheClient#builder(CredentialProvider, Configuration, Duration)}
   *
   * @param cacheName Name of the cache to store the item in
   * @param key The key under which the value is to be added.
   * @param value The value to be stored.
   * @return Future containing the result of the set operation.
   */
  public CompletableFuture<CacheSetResponse> set(String cacheName, byte[] key, byte[] value) {
    return scsDataClient.set(cacheName, key, value, null);
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
   *     building a cache client {@link CacheClient#builder(CredentialProvider, Configuration,
   *     Duration)}
   * @return Future containing the result of the set operation.
   */
  public CompletableFuture<CacheSetResponse> set(
      String cacheName, String key, String value, @Nullable Duration ttl) {
    return scsDataClient.set(cacheName, key, value, ttl);
  }

  /**
   * Sets the value in the cache. If a value for this key is already present it will be replaced by
   * the new value.
   *
   * <p>The Time to Live (TTL) seconds defaults to the parameter used when building this Cache
   * client - {@link CacheClient#builder(CredentialProvider, Configuration, Duration)}
   *
   * @param cacheName Name of the cache to store the item in
   * @param key The key under which the value is to be added.
   * @param value The value to be stored.
   * @return Future containing the result of the set operation.
   */
  public CompletableFuture<CacheSetResponse> set(String cacheName, String key, String value) {
    return scsDataClient.set(cacheName, key, value, null);
  }

  /**
   * Associates a key with a value. If a value for this key is already present it is not replaced by
   * the new value.
   *
   * @param cacheName Name of the cache to store the item in
   * @param key {String} The key under which the value is to be added.
   * @param value {String} The value to be stored.
   * @param ttl Time to Live for the item in Cache. This ttl takes precedence over the TTL used when
   *     building a cache client {@link CacheClient#builder(CredentialProvider, Configuration,
   *     Duration)}
   * @return Future containing the result of the set operation.
   */
  public CompletableFuture<CacheSetIfNotExistsResponse> setIfNotExists(
      String cacheName, String key, String value, @Nullable Duration ttl) {
    return scsDataClient.setIfNotExists(cacheName, key, value, ttl);
  }

  /**
   * Associates a key with a value. If a value for this key is already present it is not replaced by
   * the new value.
   *
   * <p>The Time to Live (TTL) seconds defaults to the parameter used when building this Cache
   * client - {@link CacheClient#builder(CredentialProvider, Configuration, Duration)}
   *
   * @param cacheName Name of the cache to store the item in
   * @param key {String} The key under which the value is to be added.
   * @param value {String} The value to be stored.
   * @return Future containing the result of the set operation.
   */
  public CompletableFuture<CacheSetIfNotExistsResponse> setIfNotExists(
      String cacheName, String key, String value) {
    return scsDataClient.setIfNotExists(cacheName, key, value, null);
  }

  /**
   * Associates a key with a value. If a value for this key is already present it is not replaced by
   * the new value.
   *
   * @param cacheName Name of the cache to store the item in
   * @param key {String} The key under which the value is to be added.
   * @param value {Byte Array} The value to be stored.
   * @param ttl Time to Live for the item in Cache. This ttl takes precedence over the TTL used when
   *     building a cache client {@link CacheClient#builder(CredentialProvider, Configuration,
   *     Duration)}
   * @return Future containing the result of the set operation.
   */
  public CompletableFuture<CacheSetIfNotExistsResponse> setIfNotExists(
      String cacheName, String key, byte[] value, @Nullable Duration ttl) {
    return scsDataClient.setIfNotExists(cacheName, key, value, ttl);
  }

  /**
   * Associates a key with a value. If a value for this key is already present it is not replaced by
   * the new value.
   *
   * <p>The Time to Live (TTL) seconds defaults to the parameter used when building this Cache
   * client - {@link CacheClient#builder(CredentialProvider, Configuration, Duration)}
   *
   * @param cacheName Name of the cache to store the item in
   * @param key {String} The key under which the value is to be added.
   * @param value {Byte Array} The value to be stored.
   * @return Future containing the result of the set operation.
   */
  public CompletableFuture<CacheSetIfNotExistsResponse> setIfNotExists(
      String cacheName, String key, byte[] value) {
    return scsDataClient.setIfNotExists(cacheName, key, value, null);
  }

  /**
   * Associates a key with a value. If a value for this key is already present it is not replaced by
   * the new value.
   *
   * @param cacheName Name of the cache to store the item in
   * @param key {Byte Array} The key under which the value is to be added.
   * @param value {String} The value to be stored.
   * @param ttl Time to Live for the item in Cache. This ttl takes precedence over the TTL used when
   *     building a cache client {@link CacheClient#builder(CredentialProvider, Configuration,
   *     Duration)}
   * @return Future containing the result of the set operation.
   */
  public CompletableFuture<CacheSetIfNotExistsResponse> setIfNotExists(
      String cacheName, byte[] key, String value, @Nullable Duration ttl) {
    return scsDataClient.setIfNotExists(cacheName, key, value, ttl);
  }

  /**
   * Associates a key with a value. If a value for this key is already present it is not replaced by
   * the new value.
   *
   * <p>The Time to Live (TTL) seconds defaults to the parameter used when building this Cache
   * client - {@link CacheClient#builder(CredentialProvider, Configuration, Duration)}
   *
   * @param cacheName Name of the cache to store the item in
   * @param key {Byte Array} The key under which the value is to be added.
   * @param value {String} The value to be stored.
   * @return Future containing the result of the set operation.
   */
  public CompletableFuture<CacheSetIfNotExistsResponse> setIfNotExists(
      String cacheName, byte[] key, String value) {
    return scsDataClient.setIfNotExists(cacheName, key, value, null);
  }

  /**
   * Associates a key with a value. If a value for this key is already present it is not replaced by
   * the new value.
   *
   * @param cacheName Name of the cache to store the item in
   * @param key {Byte Array} The key under which the value is to be added.
   * @param value {Byte Array} The value to be stored.
   * @param ttl Time to Live for the item in Cache. This ttl takes precedence over the TTL used when
   *     building a cache client {@link CacheClient#builder(CredentialProvider, Configuration,
   *     Duration)}
   * @return Future containing the result of the set operation.
   */
  public CompletableFuture<CacheSetIfNotExistsResponse> setIfNotExists(
      String cacheName, byte[] key, byte[] value, @Nullable Duration ttl) {
    return scsDataClient.setIfNotExists(cacheName, key, value, ttl);
  }

  /**
   * Associates a key with a value. If a value for this key is already present it is not replaced by
   * the new value.
   *
   * <p>The Time to Live (TTL) seconds defaults to the parameter used when building this Cache
   * client - {@link CacheClient#builder(CredentialProvider, Configuration, Duration)}
   *
   * @param cacheName Name of the cache to store the item in
   * @param key {Byte Array} The key under which the value is to be added.
   * @param value {Byte Array} The value to be stored.
   * @return Future containing the result of the set operation.
   */
  public CompletableFuture<CacheSetIfNotExistsResponse> setIfNotExists(
      String cacheName, byte[] key, byte[] value) {
    return scsDataClient.setIfNotExists(cacheName, key, value, null);
  }

  /**
   * Increments the value in the cache by an amount.
   *
   * @param cacheName Name of the cache to store the item in
   * @param field The field under which the value is to be added.
   * @param amount The amount by which the cache value is to be incremented.
   * @param ttl Time to Live for the item in Cache. This ttl takes precedence over the TTL used when
   *     building a cache client {@link CacheClient#builder(CredentialProvider, Configuration,
   *     Duration)}
   * @return Future containing the result of the increment operation.
   */
  public CompletableFuture<CacheIncrementResponse> increment(
      String cacheName, String field, long amount, @Nullable Duration ttl) {
    return scsDataClient.increment(cacheName, field, amount, ttl);
  }

  /**
   * Increments the value in the cache by an amount.
   *
   * <p>The Time to Live (TTL) seconds defaults to the parameter used when building this Cache
   * client - {@link CacheClient#builder(CredentialProvider, Configuration, Duration)}
   *
   * @param cacheName Name of the cache to store the item in
   * @param field The field under which the value is to be added.
   * @param amount The amount by which the cache value is to be incremented.
   * @return Future containing the result of the increment operation.
   */
  public CompletableFuture<CacheIncrementResponse> increment(
      String cacheName, String field, long amount) {
    return scsDataClient.increment(cacheName, field, amount, null);
  }

  /**
   * Increments the value in the cache by an amount.
   *
   * @param cacheName Name of the cache to store the item in
   * @param field The field under which the value is to be added.
   * @param amount The amount by which the cache value is to be incremented.
   * @param ttl Time to Live for the item in Cache. This ttl takes precedence over the TTL used when
   *     building a cache client {@link CacheClient#builder(CredentialProvider, Configuration,
   *     Duration)}
   * @return Future containing the result of the increment operation.
   */
  public CompletableFuture<CacheIncrementResponse> increment(
      String cacheName, byte[] field, long amount, @Nullable Duration ttl) {
    return scsDataClient.increment(cacheName, field, amount, ttl);
  }

  /**
   * Increments the value in the cache by an amount.
   *
   * <p>The Time to Live (TTL) seconds defaults to the parameter used when building this Cache
   * client - {@link CacheClient#builder(CredentialProvider, Configuration, Duration)}
   *
   * @param cacheName Name of the cache to store the item in
   * @param field The field under which the value is to be added.
   * @param amount The amount by which the cache value is to be incremented.
   * @return Future containing the result of the increment operation.
   */
  public CompletableFuture<CacheIncrementResponse> increment(
      String cacheName, byte[] field, long amount) {
    return scsDataClient.increment(cacheName, field, amount, null);
  }

  /**
   * Add an element to a set in the cache.
   *
   * <p>After this operation the set will contain the union of the element passed in and the
   * original elements of the set.
   *
   * @param cacheName Name of the cache to store the item in
   * @param setName The set to add the element to.
   * @param element The data to add to the set.
   * @param ttl TTL for the set in cache. This TTL takes precedence over the TTL used when
   *     initializing a cache client. Defaults to client TTL.
   * @return Future containing the result of the add element operation.
   */
  public CompletableFuture<CacheSetAddElementResponse> setAddElement(
      String cacheName, String setName, String element, @Nullable CollectionTtl ttl) {
    return scsDataClient.setAddElement(cacheName, setName, element, ttl);
  }

  /**
   * Add an element to a set in the cache.
   *
   * <p>After this operation the set will contain the union of the element passed in and the
   * original elements of the set.
   *
   * @param cacheName Name of the cache to store the item in
   * @param setName The set to add the element to.
   * @param element The data to add to the set.
   * @return Future containing the result of the add element operation.
   */
  public CompletableFuture<CacheSetAddElementResponse> setAddElement(
      String cacheName, String setName, String element) {
    return scsDataClient.setAddElement(cacheName, setName, element, null);
  }

  /**
   * Add an element to a set in the cache.
   *
   * <p>After this operation the set will contain the union of the element passed in and the
   * original elements of the set.
   *
   * @param cacheName Name of the cache to store the item in
   * @param setName The set to add the element to.
   * @param element The data to add to the set.
   * @param ttl TTL for the set in cache. This TTL takes precedence over the TTL used when
   *     initializing a cache client. Defaults to client TTL.
   * @return Future containing the result of the add element operation.
   */
  public CompletableFuture<CacheSetAddElementResponse> setAddElement(
      String cacheName, String setName, byte[] element, @Nullable CollectionTtl ttl) {
    return scsDataClient.setAddElement(cacheName, setName, element, ttl);
  }

  /**
   * Add an element to a set in the cache.
   *
   * <p>After this operation the set will contain the union of the element passed in and the
   * original elements of the set.
   *
   * @param cacheName Name of the cache to store the item in
   * @param setName The set to add the element to.
   * @param element The data to add to the set.
   * @return Future containing the result of the add element operation.
   */
  public CompletableFuture<CacheSetAddElementResponse> setAddElement(
      String cacheName, String setName, byte[] element) {
    return scsDataClient.setAddElement(cacheName, setName, element, null);
  }

  /**
   * Add several string elements to a set in the cache.
   *
   * <p>After this operation the set will contain the union of the elements passed in and the
   * original elements of the set.
   *
   * @param cacheName Name of the cache to store the item in
   * @param setName The set to add the elements to.
   * @param elements The data to add to the set.
   * @param ttl TTL for the set in cache. This TTL takes precedence over the TTL used when
   *     initializing a cache client. Defaults to client TTL.
   * @return Future containing the result of the add elements operation.
   */
  public CompletableFuture<CacheSetAddElementsResponse> setAddElements(
      String cacheName, String setName, Set<String> elements, @Nullable CollectionTtl ttl) {
    return scsDataClient.setAddElements(cacheName, setName, elements, ttl);
  }

  /**
   * Add several string elements to a set in the cache.
   *
   * <p>After this operation the set will contain the union of the elements passed in and the
   * original elements of the set.
   *
   * @param cacheName Name of the cache to store the item in
   * @param setName The set to add the elements to.
   * @param elements The data to add to the set.
   * @return Future containing the result of the add elements operation.
   */
  public CompletableFuture<CacheSetAddElementsResponse> setAddElements(
      String cacheName, String setName, Set<String> elements) {
    return scsDataClient.setAddElements(cacheName, setName, elements, null);
  }

  /**
   * Add several byte array elements to a set in the cache.
   *
   * <p>After this operation the set will contain the union of the elements passed in and the
   * original elements of the set.
   *
   * @param cacheName Name of the cache to store the item in
   * @param setName The set to add the elements to.
   * @param elements The data to add to the set.
   * @param ttl TTL for the set in cache. This TTL takes precedence over the TTL used when
   *     initializing a cache client. Defaults to client TTL.
   * @return Future containing the result of the add elements operation.
   */
  public CompletableFuture<CacheSetAddElementsResponse> setAddElementsByteArray(
      String cacheName, String setName, Set<byte[]> elements, @Nullable CollectionTtl ttl) {
    return scsDataClient.setAddElementsByteArray(cacheName, setName, elements, ttl);
  }

  /**
   * Add several byte array elements to a set in the cache.
   *
   * <p>After this operation the set will contain the union of the elements passed in and the
   * original elements of the set.
   *
   * @param cacheName Name of the cache to store the item in
   * @param setName The set to add the elements to.
   * @param elements The data to add to the set.
   * @return Future containing the result of the add elements operation.
   */
  public CompletableFuture<CacheSetAddElementsResponse> setAddElementsByteArray(
      String cacheName, String setName, Set<byte[]> elements) {
    return scsDataClient.setAddElementsByteArray(cacheName, setName, elements, null);
  }

  /**
   * Remove an element from a set.
   *
   * @param cacheName Name of the cache containing the set.
   * @param setName The set to remove the element from.
   * @param element The value to remove from the set.
   * @return Future containing the result of the remove element operation.
   */
  public CompletableFuture<CacheSetRemoveElementResponse> setRemoveElement(
      String cacheName, String setName, String element) {
    return scsDataClient.setRemoveElement(cacheName, setName, element);
  }

  /**
   * Remove an element from a set.
   *
   * @param cacheName Name of the cache containing the set.
   * @param setName The set to remove the element from.
   * @param element The value to remove from the set.
   * @return Future containing the result of the remove element operation.
   */
  public CompletableFuture<CacheSetRemoveElementResponse> setRemoveElement(
      String cacheName, String setName, byte[] element) {
    return scsDataClient.setRemoveElement(cacheName, setName, element);
  }

  /**
   * Remove several elements from a set.
   *
   * @param cacheName Name of the cache containing the set.
   * @param setName The set to remove the elements from.
   * @param elements The values to remove from the set.
   * @return Future containing the result of the remove elements operation.
   */
  public CompletableFuture<CacheSetRemoveElementsResponse> setRemoveElements(
      String cacheName, String setName, Set<String> elements) {
    return scsDataClient.setRemoveElements(cacheName, setName, elements);
  }

  /**
   * Remove several elements from a set.
   *
   * @param cacheName Name of the cache containing the set.
   * @param setName The set to remove the elements from.
   * @param elements The value to remove from the set.
   * @return Future containing the result of the remove elements operation.
   */
  public CompletableFuture<CacheSetRemoveElementsResponse> setRemoveElementsByteArray(
      String cacheName, String setName, Set<byte[]> elements) {
    return scsDataClient.setRemoveElementsByteArray(cacheName, setName, elements);
  }

  /**
   * Fetch an entire set from the cache.
   *
   * @param cacheName Name of the cache to perform the lookup in
   * @param setName The set to fetch.
   * @return Future containing the result of the fetch operation.
   */
  public CompletableFuture<CacheSetFetchResponse> setFetch(String cacheName, String setName) {
    return scsDataClient.setFetch(cacheName, setName);
  }

  /**
   * Adds an element to the given sorted set. If the element already exists, its score is updated.
   * Creates the sorted set if it does not exist.
   *
   * @param cacheName - The cache containing the sorted set.
   * @param sortedSetName - The sorted set to add to.
   * @param element - The element to add.
   * @param score - The score to assign to the element.
   * @param ttl TTL for the set in cache. This TTL takes precedence over the TTL used when
   *     initializing a cache client. Defaults to client TTL.
   * @return Future containing the result of the put element operation.
   */
  public CompletableFuture<CacheSortedSetPutElementResponse> sortedSetPutElement(
      String cacheName,
      String sortedSetName,
      String element,
      double score,
      @Nullable CollectionTtl ttl) {
    return scsDataClient.sortedSetPutElement(cacheName, sortedSetName, element, score, ttl);
  }

  /**
   * Adds an element to the given sorted set. If the element already exists, its score is updated.
   * Creates the sorted set if it does not exist.
   *
   * @param cacheName - The cache containing the sorted set.
   * @param sortedSetName - The sorted set to add to.
   * @param element - The element to add.
   * @param score - The score to assign to the element.
   * @return Future containing the result of the put element operation.
   */
  public CompletableFuture<CacheSortedSetPutElementResponse> sortedSetPutElement(
      String cacheName, String sortedSetName, String element, double score) {
    return scsDataClient.sortedSetPutElement(cacheName, sortedSetName, element, score, null);
  }

  /**
   * Adds an element to the given sorted set. If the element already exists, its score is updated.
   * Creates the sorted set if it does not exist.
   *
   * @param cacheName - The cache containing the sorted set.
   * @param sortedSetName - The sorted set to add to.
   * @param element - The element to add.
   * @param score - The score to assign to the element.
   * @param ttl TTL for the set in cache. This TTL takes precedence over the TTL used when
   *     initializing a cache client. Defaults to client TTL.
   * @return Future containing the result of the put element operation.
   */
  public CompletableFuture<CacheSortedSetPutElementResponse> sortedSetPutElement(
      String cacheName,
      String sortedSetName,
      byte[] element,
      double score,
      @Nullable CollectionTtl ttl) {
    return scsDataClient.sortedSetPutElement(cacheName, sortedSetName, element, score, ttl);
  }

  /**
   * Adds an element to the given sorted set. If the element already exists, its score is updated.
   * Creates the sorted set if it does not exist.
   *
   * @param cacheName - The cache containing the sorted set.
   * @param sortedSetName - The sorted set to add to.
   * @param element - The element to add.
   * @param score - The score to assign to the element.
   * @return Future containing the result of the put element operation.
   */
  public CompletableFuture<CacheSortedSetPutElementResponse> sortedSetPutElement(
      String cacheName, String sortedSetName, byte[] element, double score) {
    return scsDataClient.sortedSetPutElement(cacheName, sortedSetName, element, score, null);
  }

  /**
   * Adds elements to the given sorted set. If the elements already exist, their scores are updated.
   * Creates the sorted set if it does not exist.
   *
   * @param cacheName - The cache containing the sorted set.
   * @param sortedSetName - The sorted set to add to.
   * @param elements - The value to score map to add to the sorted set.
   * @param ttl TTL for the set in cache. This TTL takes precedence over the TTL used when
   *     initializing a cache client. Defaults to client TTL.
   * @return Future containing the result of the put elements operation.
   */
  public CompletableFuture<CacheSortedSetPutElementsResponse> sortedSetPutElements(
      String cacheName,
      String sortedSetName,
      Map<String, Double> elements,
      @Nullable CollectionTtl ttl) {
    return scsDataClient.sortedSetPutElements(cacheName, sortedSetName, elements, ttl);
  }

  /**
   * Adds elements to the given sorted set. If the elements already exist, their scores are updated.
   * * Creates the sorted set if it does not exist.
   *
   * @param cacheName - The cache containing the sorted set.
   * @param sortedSetName - The sorted set to add to.
   * @param elements - The value to score map to add to the sorted set.
   * @return Future containing the result of the put elements operation.
   */
  public CompletableFuture<CacheSortedSetPutElementsResponse> sortedSetPutElements(
      String cacheName, String sortedSetName, Map<String, Double> elements) {
    return scsDataClient.sortedSetPutElements(cacheName, sortedSetName, elements, null);
  }

  /**
   * Adds elements to the given sorted set. If the elements already exist, their scores are updated.
   * Creates the sorted set if it does not exist.
   *
   * @param cacheName - The cache containing the sorted set.
   * @param sortedSetName - The sorted set to add to.
   * @param elements - The value to score map to add to the sorted set.
   * @param ttl TTL for the set in cache. This TTL takes precedence over the TTL used when
   *     initializing a cache client. Defaults to client TTL.
   * @return Future containing the result of the put elements operation.
   */
  public CompletableFuture<CacheSortedSetPutElementsResponse> sortedSetPutElementsByteArray(
      String cacheName,
      String sortedSetName,
      Map<byte[], Double> elements,
      @Nullable CollectionTtl ttl) {
    return scsDataClient.sortedSetPutElementsByteArray(cacheName, sortedSetName, elements, ttl);
  }

  /**
   * Adds elements to the given sorted set. If the elements already exist, their scores are updated.
   * * Creates the sorted set if it does not exist.
   *
   * @param cacheName - The cache containing the sorted set.
   * @param sortedSetName - The sorted set to add to.
   * @param elements - The value to score map to add to the sorted set.
   * @return Future containing the result of the put elements operation.
   */
  public CompletableFuture<CacheSortedSetPutElementsResponse> sortedSetPutElementsByteArray(
      String cacheName, String sortedSetName, Map<byte[], Double> elements) {
    return scsDataClient.sortedSetPutElementsByteArray(cacheName, sortedSetName, elements, null);
  }

  /**
   * Fetch the elements in the given sorted set by index (rank).
   *
   * @param cacheName - The cache containing the sorted set.
   * @param sortedSetName - The sorted set to fetch from.
   * @param startRank - The rank of the first element to fetch. Defaults to 0. This rank is
   *     inclusive, i.e. the element at this rank will be fetched.
   * @param endRank - The rank of the last element to fetch. This rank is exclusive, ie the element
   *     at this rank will not be fetched. Defaults to null, which fetches up until and including
   *     the last element.
   * @return Future containing the result of the fetch operation.
   */
  public CompletableFuture<CacheSortedSetFetchResponse> sortedSetFetchByRank(
      String cacheName,
      String sortedSetName,
      @Nullable Integer startRank,
      @Nullable Integer endRank) {
    return scsDataClient.sortedSetFetchByRank(cacheName, sortedSetName, startRank, endRank);
  }

  /**
   * Fetch all elements in the given sorted set by index (rank) in ascending order.
   *
   * @param cacheName - The cache containing the sorted set.
   * @param sortedSetName - The sorted set to fetch from.
   * @return Future containing the result of the fetch operation.
   */
  public CompletableFuture<CacheSortedSetFetchResponse> sortedSetFetchByRank(
      String cacheName, String sortedSetName) {
    return scsDataClient.sortedSetFetchByRank(cacheName, sortedSetName, null, null);
  }

  /**
   * Fetch the elements in the given sorted set by score.
   *
   * @param cacheName - The cache containing the sorted set.
   * @param sortedSetName - The sorted set to fetch from.
   * @param minScore - The minimum score (inclusive) of the elements to fetch. Defaults to negative
   *     infinity.
   * @param maxScore - The maximum score (inclusive) of the elements to fetch. Defaults to positive
   *     infinity.
   * @param offset - The number of elements to skip before returning the first element. Defaults to
   *     0. Note: this is not the rank of the first element to return, but the number of elements of
   *     the result set to skip before returning the first element.
   * @param count - The maximum number of elements to return. Defaults to all elements.
   * @return Future containing the result of the fetch operation.
   */
  public CompletableFuture<CacheSortedSetFetchResponse> sortedSetFetchByScore(
      String cacheName,
      String sortedSetName,
      @Nullable Double minScore,
      @Nullable Double maxScore,
      @Nullable Integer offset,
      @Nullable Integer count) {
    return scsDataClient.sortedSetFetchByScore(
        cacheName, sortedSetName, minScore, maxScore, offset, count);
  }

  /**
   * Fetch all elements in the given sorted set by score in ascending order.
   *
   * @param cacheName - The cache containing the sorted set.
   * @param sortedSetName - The sorted set to fetch from.
   * @param offset - The number of elements to skip before returning the first element. Defaults to
   *     0. Note: this is not the rank of the first element to return, but the number of elements of
   *     the result set to skip before returning the first element.
   * @param count - The maximum number of elements to return. Defaults to all elements.
   * @return Future containing the result of the fetch operation.
   */
  public CompletableFuture<CacheSortedSetFetchResponse> sortedSetFetchByScore(
      String cacheName, String sortedSetName, @Nullable Integer offset, @Nullable Integer count) {
    return scsDataClient.sortedSetFetchByScore(cacheName, sortedSetName, null, null, offset, count);
  }

  /**
   * Concatenates values to the back of the list.
   *
   * @param cacheName Name of the cache to store the item in
   * @param listName The list in which the value is to be added.
   * @param values The elements to add to the list.
   * @param truncateFrontToSize If the list exceeds this length, remove excess from the front of the
   *     list. Must be positive.
   * @param ttl Time to Live for the item in Cache. This ttl takes precedence over the TTL used when
   *     building a cache client {@link CacheClient#builder(CredentialProvider, Configuration,
   *     Duration)}
   * @return Future containing the result of the list concatenate back operation.
   */
  public CompletableFuture<CacheListConcatenateBackResponse> listConcatenateBack(
      String cacheName,
      String listName,
      List<String> values,
      int truncateFrontToSize,
      @Nullable CollectionTtl ttl) {
    return scsDataClient.listConcatenateBack(cacheName, listName, values, truncateFrontToSize, ttl);
  }

  /**
   * Concatenates values to the back of the list.
   *
   * @param cacheName Name of the cache to store the item in
   * @param listName The list in which the value is to be added.
   * @param values The elements to add to the list.
   * @param truncateFrontToSize If the list exceeds this length, remove excess from the front of the
   *     list. Must be positive.
   * @return Future containing the result of the list concatenate back operation.
   */
  public CompletableFuture<CacheListConcatenateBackResponse> listConcatenateBack(
      String cacheName, String listName, List<String> values, int truncateFrontToSize) {
    return scsDataClient.listConcatenateBack(
        cacheName, listName, values, truncateFrontToSize, null);
  }

  /**
   * Concatenates values to the back of the list.
   *
   * @param cacheName Name of the cache to store the item in
   * @param listName The list in which the value is to be added.
   * @param values The elements to add to the list.
   * @param truncateFrontToSize If the list exceeds this length, remove excess from the front of the
   *     list. Must be positive.
   * @param ttl Time to Live for the item in Cache. This ttl takes precedence over the TTL used when
   *     building a cache client {@link CacheClient#builder(CredentialProvider, Configuration,
   *     Duration)}
   * @return Future containing the result of the list concatenate back operation.
   */
  public CompletableFuture<CacheListConcatenateBackResponse> listConcatenateBackByteArray(
      String cacheName,
      String listName,
      List<byte[]> values,
      int truncateFrontToSize,
      @Nullable CollectionTtl ttl) {
    return scsDataClient.listConcatenateBackByteArray(
        cacheName, listName, values, truncateFrontToSize, ttl);
  }

  /**
   * Concatenates values to the back of the list.
   *
   * @param cacheName Name of the cache to store the item in
   * @param listName The list in which the value is to be added.
   * @param values The elements to add to the list.
   * @param truncateFrontToSize If the list exceeds this length, remove excess from the front of the
   *     list. Must be positive.
   * @return Future containing the result of the list concatenate back operation.
   */
  public CompletableFuture<CacheListConcatenateBackResponse> listConcatenateBackByteArray(
      String cacheName, String listName, List<byte[]> values, int truncateFrontToSize) {
    return scsDataClient.listConcatenateBackByteArray(
        cacheName, listName, values, truncateFrontToSize, null);
  }

  /**
   * Concatenates values to the front of the list.
   *
   * @param cacheName Name of the cache to store the item in
   * @param listName The list in which the value is to be added.
   * @param values The elements to add to the list.
   * @param truncateBackToSize If the list exceeds this length, remove excess from the front of the
   *     list. Must be positive.
   * @param ttl Time to Live for the item in Cache. This ttl takes precedence over the TTL used when
   *     building a cache client {@link CacheClient#builder(CredentialProvider, Configuration,
   *     Duration)}
   * @return Future containing the result of the list concatenate front operation.
   */
  public CompletableFuture<CacheListConcatenateFrontResponse> listConcatenateFront(
      String cacheName,
      String listName,
      List<String> values,
      int truncateBackToSize,
      @Nullable CollectionTtl ttl) {
    return scsDataClient.listConcatenateFront(cacheName, listName, values, truncateBackToSize, ttl);
  }

  /**
   * Concatenates values to the front of the list.
   *
   * @param cacheName Name of the cache to store the item in
   * @param listName The list in which the value is to be added.
   * @param values The elements to add to the list.
   * @param truncateBackToSize If the list exceeds this length, remove excess from the front of the
   *     list. Must be positive.
   * @return Future containing the result of the list concatenate front operation.
   */
  public CompletableFuture<CacheListConcatenateFrontResponse> listConcatenateFront(
      String cacheName, String listName, List<String> values, int truncateBackToSize) {
    return scsDataClient.listConcatenateFront(
        cacheName, listName, values, truncateBackToSize, null);
  }

  /**
   * Concatenates values to the front of the list.
   *
   * @param cacheName Name of the cache to store the item in
   * @param listName The list in which the value is to be added.
   * @param values The elements to add to the list.
   * @param truncateBackToSize If the list exceeds this length, remove excess from the front of the
   *     list. Must be positive.
   * @param ttl Time to Live for the item in Cache. This ttl takes precedence over the TTL used when
   *     building a cache client {@link CacheClient#builder(CredentialProvider, Configuration,
   *     Duration)}
   * @return Future containing the result of the list concatenate front operation.
   */
  public CompletableFuture<CacheListConcatenateFrontResponse> listConcatenateFrontByteArray(
      String cacheName,
      String listName,
      List<byte[]> values,
      int truncateBackToSize,
      @Nullable CollectionTtl ttl) {
    return scsDataClient.listConcatenateFrontByteArray(
        cacheName, listName, values, truncateBackToSize, ttl);
  }

  /**
   * Concatenates values to the front of the list.
   *
   * @param cacheName Name of the cache to store the item in
   * @param listName The list in which the value is to be added.
   * @param values The elements to add to the list.
   * @param truncateBackToSize If the list exceeds this length, remove excess from the front of the
   *     list. Must be positive.
   * @return Future containing the result of the list concatenate front operation.
   */
  public CompletableFuture<CacheListConcatenateFrontResponse> listConcatenateFrontByteArray(
      String cacheName, String listName, List<byte[]> values, int truncateBackToSize) {
    return scsDataClient.listConcatenateFrontByteArray(
        cacheName, listName, values, truncateBackToSize, null);
  }

  /**
   * Fetches all elements of the given list.
   *
   * @param cacheName - The cache containing the list.
   * @param listName - The list to fetch.
   * @param startIndex - Start inclusive index for fetch operation.
   * @param endIndex - End exclusive index for fetch operation.
   * @return Future containing the result of the list fetch back operation.
   */
  public CompletableFuture<CacheListFetchResponse> listFetch(
      String cacheName, String listName, Integer startIndex, Integer endIndex) {
    return scsDataClient.listFetch(cacheName, listName, startIndex, endIndex);
  }

  /**
   * Fetches length of the given list.
   *
   * @param cacheName - The cache containing the list.
   * @param listName - The list to fetch.
   * @return Future containing the result of the list length back operation.
   */
  public CompletableFuture<CacheListLengthResponse> listLength(String cacheName, String listName) {
    return scsDataClient.listLength(cacheName, listName);
  }

  /**
   * Fetches and removes the value from the back of the given list.
   *
   * @param cacheName - The cache containing the list.
   * @param listName - The list to fetch the value from.
   * @return Future containing the result of the list pop back operation.
   */
  public CompletableFuture<CacheListPopBackResponse> listPopBack(
      String cacheName, String listName) {
    return scsDataClient.listPopBack(cacheName, listName);
  }

  /**
   * Fetches and removes the value from the front of the given list.
   *
   * @param cacheName - The cache containing the list.
   * @param listName - The list to fetch the value from.
   * @return Future containing the result of the list pop front operation.
   */
  public CompletableFuture<CacheListPopFrontResponse> listPopFront(
      String cacheName, String listName) {
    return scsDataClient.listPopFront(cacheName, listName);
  }

  /**
   * Pushes a value to the back of the list.
   *
   * @param cacheName Name of the cache to store the value in
   * @param listName The list in which the value is to be added.
   * @param value The element to add to the list.
   * @param truncateFrontToSize If the list exceeds this length, remove excess from the front of the
   *     list. Must be positive.
   * @param ttl Time to Live for the item in Cache. This ttl takes precedence over the TTL used when
   *     building a cache client {@link CacheClient#builder(CredentialProvider, Configuration,
   *     Duration)}
   * @return Future containing the result of the list push back operation.
   */
  public CompletableFuture<CacheListPushBackResponse> listPushBack(
      String cacheName,
      String listName,
      String value,
      int truncateFrontToSize,
      @Nullable CollectionTtl ttl) {
    return scsDataClient.listPushBack(cacheName, listName, value, truncateFrontToSize, ttl);
  }

  /**
   * Pushes a value to the back of the list.
   *
   * @param cacheName Name of the cache to store the value in
   * @param listName The list in which the value is to be added.
   * @param value The element to add to the list.
   * @param truncateFrontToSize If the list exceeds this length, remove excess from the front of the
   *     list. Must be positive.
   * @return Future containing the result of the list push back operation.
   */
  public CompletableFuture<CacheListPushBackResponse> listPushBack(
      String cacheName, String listName, String value, int truncateFrontToSize) {
    return scsDataClient.listPushBack(cacheName, listName, value, truncateFrontToSize, null);
  }

  /**
   * Pushes a value to the back of the list.
   *
   * @param cacheName Name of the cache to store the value in
   * @param listName The list in which the value is to be added.
   * @param value The element to add to the list.
   * @param truncateFrontToSize If the list exceeds this length, remove excess from the front of the
   *     list. Must be positive.
   * @param ttl Time to Live for the item in Cache. This ttl takes precedence over the TTL used when
   *     building a cache client {@link CacheClient#builder(CredentialProvider, Configuration,
   *     Duration)}
   * @return Future containing the result of the list push back operation.
   */
  public CompletableFuture<CacheListPushBackResponse> listPushBack(
      String cacheName,
      String listName,
      byte[] value,
      int truncateFrontToSize,
      @Nullable CollectionTtl ttl) {
    return scsDataClient.listPushBack(cacheName, listName, value, truncateFrontToSize, ttl);
  }

  /**
   * Pushes a value to the back of the list.
   *
   * @param cacheName Name of the cache to store the value in
   * @param listName The list in which the value is to be added.
   * @param value The element to add to the list.
   * @param truncateFrontToSize If the list exceeds this length, remove excess from the front of the
   *     list. Must be positive.
   * @return Future containing the result of the list push back operation.
   */
  public CompletableFuture<CacheListPushBackResponse> listPushBack(
      String cacheName, String listName, byte[] value, int truncateFrontToSize) {
    return scsDataClient.listPushBack(cacheName, listName, value, truncateFrontToSize, null);
  }

  /**
   * Pushes a value to the front of the list.
   *
   * @param cacheName Name of the cache to store the value in
   * @param listName The list in which the value is to be added.
   * @param value The element to add to the list.
   * @param truncateBackToSize If the list exceeds this length, remove excess from the front of the
   *     list. Must be positive.
   * @param ttl Time to Live for the item in Cache. This ttl takes precedence over the TTL used when
   *     building a cache client {@link CacheClient#builder(CredentialProvider, Configuration,
   *     Duration)}
   * @return Future containing the result of the list push front operation.
   */
  public CompletableFuture<CacheListPushFrontResponse> listPushFront(
      String cacheName,
      String listName,
      String value,
      int truncateBackToSize,
      @Nullable CollectionTtl ttl) {
    return scsDataClient.listPushFront(cacheName, listName, value, truncateBackToSize, ttl);
  }

  /**
   * Pushes a value to the front of the list.
   *
   * @param cacheName Name of the cache to store the value in
   * @param listName The list in which the value is to be added.
   * @param value The element to add to the list.
   * @param truncateBackToSize If the list exceeds this length, remove excess from the front of the
   *     list. Must be positive.
   * @return Future containing the result of the list push front operation.
   */
  public CompletableFuture<CacheListPushFrontResponse> listPushFront(
      String cacheName, String listName, String value, int truncateBackToSize) {
    return scsDataClient.listPushFront(cacheName, listName, value, truncateBackToSize, null);
  }

  /**
   * Pushes a value to the front of the list.
   *
   * @param cacheName Name of the cache to store the value in
   * @param listName The list in which the value is to be added.
   * @param value The element to add to the list.
   * @param truncateBackToSize If the list exceeds this length, remove excess from the front of the
   *     list. Must be positive.
   * @param ttl Time to Live for the item in Cache. This ttl takes precedence over the TTL used when
   *     building a cache client {@link CacheClient#builder(CredentialProvider, Configuration,
   *     Duration)}
   * @return Future containing the result of the list push front operation.
   */
  public CompletableFuture<CacheListPushFrontResponse> listPushFront(
      String cacheName,
      String listName,
      byte[] value,
      int truncateBackToSize,
      @Nullable CollectionTtl ttl) {
    return scsDataClient.listPushFront(cacheName, listName, value, truncateBackToSize, ttl);
  }

  /**
   * Pushes a value to the front of the list.
   *
   * @param cacheName Name of the cache to store the value in
   * @param listName The list in which the value is to be added.
   * @param value The element to add to the list.
   * @param truncateBackToSize If the list exceeds this length, remove excess from the front of the
   *     list. Must be positive.
   * @return Future containing the result of the list push front operation.
   */
  public CompletableFuture<CacheListPushFrontResponse> listPushFront(
      String cacheName, String listName, byte[] value, int truncateBackToSize) {
    return scsDataClient.listPushFront(cacheName, listName, value, truncateBackToSize, null);
  }

  /**
   * Removes value from the given list.
   *
   * @param cacheName The cache containing the list.
   * @param listName The list in which the value is to be removed.
   * @param value The element to add to the list.
   * @return Future containing the result of the list remove value operation.
   */
  public CompletableFuture<CacheListRemoveValueResponse> listRemoveValue(
      String cacheName, String listName, String value) {
    return scsDataClient.listRemoveValue(cacheName, listName, value);
  }

  /**
   * Removes value from the given list.
   *
   * @param cacheName The cache containing the list.
   * @param listName The list in which the value is to be removed.
   * @param value The element to add to the list.
   * @return Future containing the result of the list remove value operation.
   */
  public CompletableFuture<CacheListRemoveValueResponse> listRemoveValue(
      String cacheName, String listName, byte[] value) {
    return scsDataClient.listRemoveValue(cacheName, listName, value);
  }

  /**
   * Retain only the elements within the given indices.
   *
   * @param cacheName The cache containing the list.
   * @param listName The list to retain the value from.
   * @param startIndex - Start inclusive index for list retain operation.
   * @param endIndex - End exclusive index for list retain operation.
   * @return Future containing the result of the list retain value operation.
   */
  public CompletableFuture<CacheListRetainResponse> listRetain(
      String cacheName, String listName, Integer startIndex, Integer endIndex) {
    return scsDataClient.listRetain(cacheName, listName, startIndex, endIndex);
  }

  /**
   * Fetches all elements of the given dictionary.
   *
   * @param cacheName - The cache containing the dictionary.
   * @param dictionaryName - The dictionary to fetch.
   * @return Future containing the result of the dictionary fetch operation.
   */
  public CompletableFuture<CacheDictionaryFetchResponse> dictionaryFetch(
      String cacheName, String dictionaryName) {
    return scsDataClient.dictionaryFetch(cacheName, dictionaryName);
  }

  /**
   * Sets a field in the given dictionary.
   *
   * @param cacheName - The cache containing the dictionary.
   * @param dictionaryName - The dictionary to set the field in.
   * @param field - The field to set.
   * @param value - The value to set.
   * @param ttl Time to Live for the item in Cache. This ttl takes precedence over the TTL used when
   *     building a cache client {@link CacheClient#builder(CredentialProvider, Configuration,
   *     Duration)}
   * @return Future containing the result of the dictionary set field operation.
   */
  public CompletableFuture<CacheDictionarySetFieldResponse> dictionarySetField(
      String cacheName, String dictionaryName, String field, String value, CollectionTtl ttl) {
    return scsDataClient.dictionarySetField(cacheName, dictionaryName, field, value, ttl);
  }

  /**
   * Sets a field in the given dictionary.
   *
   * @param cacheName - The cache containing the dictionary.
   * @param dictionaryName - The dictionary to set the field in.
   * @param field - The field to set.
   * @param value - The value to set.
   * @return Future containing the result of the dictionary set field operation.
   */
  public CompletableFuture<CacheDictionarySetFieldResponse> dictionarySetField(
      String cacheName, String dictionaryName, String field, String value) {
    return scsDataClient.dictionarySetField(cacheName, dictionaryName, field, value, null);
  }

  /**
   * Sets a field in the given dictionary.
   *
   * @param cacheName - The cache containing the dictionary.
   * @param dictionaryName - The dictionary to set the field in.
   * @param field - The field to set.
   * @param value - The value to set.
   * @param ttl Time to Live for the item in Cache. This ttl takes precedence over the TTL used when
   *     building a cache client {@link CacheClient#builder(CredentialProvider, Configuration,
   *     Duration)}
   * @return Future containing the result of the dictionary set field operation.
   */
  public CompletableFuture<CacheDictionarySetFieldResponse> dictionarySetField(
      String cacheName, String dictionaryName, String field, byte[] value, CollectionTtl ttl) {
    return scsDataClient.dictionarySetField(cacheName, dictionaryName, field, value, ttl);
  }

  /**
   * Sets a field in the given dictionary.
   *
   * @param cacheName - The cache containing the dictionary.
   * @param dictionaryName - The dictionary to set the field in.
   * @param field - The field to set.
   * @param value - The value to set.
   * @return Future containing the result of the dictionary set field operation.
   */
  public CompletableFuture<CacheDictionarySetFieldResponse> dictionarySetField(
      String cacheName, String dictionaryName, String field, byte[] value) {
    return scsDataClient.dictionarySetField(cacheName, dictionaryName, field, value, null);
  }

  /**
   * Sets a field in the given dictionary.
   *
   * @param cacheName - The cache containing the dictionary.
   * @param dictionaryName - The dictionary to set the field in.
   * @param field - The field to set.
   * @param value - The value to set.
   * @param ttl Time to Live for the item in Cache. This ttl takes precedence over the TTL used when
   *     building a cache client {@link CacheClient#builder(CredentialProvider, Configuration,
   *     Duration)}
   * @return Future containing the result of the dictionary set field operation.
   */
  public CompletableFuture<CacheDictionarySetFieldResponse> dictionarySetField(
      String cacheName, String dictionaryName, byte[] field, String value, CollectionTtl ttl) {
    return scsDataClient.dictionarySetField(cacheName, dictionaryName, field, value, ttl);
  }

  /**
   * Sets a field in the given dictionary.
   *
   * @param cacheName - The cache containing the dictionary.
   * @param dictionaryName - The dictionary to set the field in.
   * @param field - The field to set.
   * @param value - The value to set.
   * @return Future containing the result of the dictionary set field operation.
   */
  public CompletableFuture<CacheDictionarySetFieldResponse> dictionarySetField(
      String cacheName, String dictionaryName, byte[] field, String value) {
    return scsDataClient.dictionarySetField(cacheName, dictionaryName, field, value, null);
  }

  /**
   * Sets a field in the given dictionary.
   *
   * @param cacheName - The cache containing the dictionary.
   * @param dictionaryName - The dictionary to set the field in.
   * @param field - The field to set.
   * @param value - The value to set.
   * @param ttl Time to Live for the item in Cache. This ttl takes precedence over the TTL used when
   *     building a cache client {@link CacheClient#builder(CredentialProvider, Configuration,
   *     Duration)}
   * @return Future containing the result of the dictionary set field operation.
   */
  public CompletableFuture<CacheDictionarySetFieldResponse> dictionarySetField(
      String cacheName, String dictionaryName, byte[] field, byte[] value, CollectionTtl ttl) {
    return scsDataClient.dictionarySetField(cacheName, dictionaryName, field, value, ttl);
  }

  /**
   * Sets a field in the given dictionary.
   *
   * @param cacheName - The cache containing the dictionary.
   * @param dictionaryName - The dictionary to set the field in.
   * @param field - The field to set.
   * @param value - The value to set.
   * @return Future containing the result of the dictionary set field operation.
   */
  public CompletableFuture<CacheDictionarySetFieldResponse> dictionarySetField(
      String cacheName, String dictionaryName, byte[] field, byte[] value) {
    return scsDataClient.dictionarySetField(cacheName, dictionaryName, field, value, null);
  }

  /**
   * Sets all the fields in the given dictionary.
   *
   * @param cacheName - The cache containing the list.
   * @param dictionaryName - The dictionary to set the field in.
   * @param elements - The fields to set.
   * @param ttl Time to Live for the item in Cache. This ttl takes precedence over the TTL used when
   *     building a cache client {@link CacheClient#builder(CredentialProvider, Configuration,
   *     Duration)}
   * @return Future containing the result of the dictionary set fields operation.
   */
  public CompletableFuture<CacheDictionarySetFieldsResponse> dictionarySetFields(
      String cacheName, String dictionaryName, Map<String, String> elements, CollectionTtl ttl) {
    return scsDataClient.dictionarySetFields(cacheName, dictionaryName, elements, ttl);
  }

  /**
   * Sets all the fields in the given dictionary.
   *
   * <p>The Time to Live (TTL) seconds defaults to the parameter used when building this Cache
   * client - {@link CacheClient#builder(CredentialProvider, Configuration, Duration)}
   *
   * @param cacheName - The cache containing the dictionary.
   * @param dictionaryName - The dictionary to set the field in.
   * @param elements - The fields to set.
   * @return Future containing the result of the dictionary set fields operation.
   */
  public CompletableFuture<CacheDictionarySetFieldsResponse> dictionarySetFields(
      String cacheName, String dictionaryName, Map<String, String> elements) {
    return scsDataClient.dictionarySetFields(cacheName, dictionaryName, elements, null);
  }

  /**
   * Sets all the fields in the given dictionary.
   *
   * @param cacheName - The cache containing the dictionary.
   * @param dictionaryName - The dictionary to set the field in.
   * @param elements - The fields to set.
   * @param ttl Time to Live for the item in Cache. This ttl takes precedence over the TTL used when
   *     building a cache client {@link CacheClient#builder(CredentialProvider, Configuration,
   *     Duration)}
   * @return Future containing the result of the dictionary set fields operation.
   */
  public CompletableFuture<CacheDictionarySetFieldsResponse> dictionarySetFieldsStringBytes(
      String cacheName, String dictionaryName, Map<String, byte[]> elements, CollectionTtl ttl) {
    return scsDataClient.dictionarySetFieldsStringBytes(cacheName, dictionaryName, elements, ttl);
  }

  /**
   * Sets all the fields in the given dictionary.
   *
   * <p>The Time to Live (TTL) seconds defaults to the parameter used when building this Cache
   * client - {@link CacheClient#builder(CredentialProvider, Configuration, Duration)}
   *
   * @param cacheName - The cache containing the dictionary.
   * @param dictionaryName - The dictionary to set the field in.
   * @param elements - The fields to set.
   * @return Future containing the result of the dictionary set fields operation.
   */
  public CompletableFuture<CacheDictionarySetFieldsResponse> dictionarySetFieldsStringBytes(
      String cacheName, String dictionaryName, Map<String, byte[]> elements) {
    return scsDataClient.dictionarySetFieldsStringBytes(cacheName, dictionaryName, elements, null);
  }

  /**
   * Sets all the fields in the given dictionary.
   *
   * @param cacheName - The cache containing the dictionary.
   * @param dictionaryName - The dictionary to set the field in.
   * @param elements - The fields to set.
   * @param ttl Time to Live for the item in Cache. This ttl takes precedence over the TTL used when
   *     building a cache client {@link CacheClient#builder(CredentialProvider, Configuration,
   *     Duration)}
   * @return Future containing the result of the dictionary set fields operation.
   */
  public CompletableFuture<CacheDictionarySetFieldsResponse> dictionarySetFieldsBytesString(
      String cacheName, String dictionaryName, Map<byte[], String> elements, CollectionTtl ttl) {
    return scsDataClient.dictionarySetFieldsBytesString(cacheName, dictionaryName, elements, ttl);
  }

  /**
   * Sets all the fields in the given dictionary.
   *
   * <p>The Time to Live (TTL) seconds defaults to the parameter used when building this Cache
   * client - {@link CacheClient#builder(CredentialProvider, Configuration, Duration)}
   *
   * @param cacheName - The cache containing the dictionary.
   * @param dictionaryName - The dictionary to set the field in.
   * @param elements - The fields to set.
   * @return Future containing the result of the dictionary set fields operation.
   */
  public CompletableFuture<CacheDictionarySetFieldsResponse> dictionarySetFieldsBytesString(
      String cacheName, String dictionaryName, Map<byte[], String> elements) {
    return scsDataClient.dictionarySetFieldsBytesString(cacheName, dictionaryName, elements, null);
  }

  /**
   * Sets all the fields in the given dictionary.
   *
   * @param cacheName - The cache containing the dictionary.
   * @param dictionaryName - The dictionary to set the field in.
   * @param elements - The fields to set.
   * @param ttl Time to Live for the item in Cache. This ttl takes precedence over the TTL used when
   *     building a cache client {@link CacheClient#builder(CredentialProvider, Configuration,
   *     Duration)}
   * @return Future containing the result of the dictionary set fields operation.
   */
  public CompletableFuture<CacheDictionarySetFieldsResponse> dictionarySetFieldsBytesBytes(
      String cacheName, String dictionaryName, Map<byte[], byte[]> elements, CollectionTtl ttl) {
    return scsDataClient.dictionarySetFieldsBytesBytes(cacheName, dictionaryName, elements, ttl);
  }

  /**
   * Sets all the fields in the given dictionary.
   *
   * <p>The Time to Live (TTL) seconds defaults to the parameter used when building this Cache
   * client - {@link CacheClient#builder(CredentialProvider, Configuration, Duration)}
   *
   * @param cacheName - The cache containing the dictionary.
   * @param dictionaryName - The dictionary to set the field in.
   * @param elements - The fields to set.
   * @return Future containing the result of the dictionary set fields operation.
   */
  public CompletableFuture<CacheDictionarySetFieldsResponse> dictionarySetFieldsBytesBytes(
      String cacheName, String dictionaryName, Map<byte[], byte[]> elements) {
    return scsDataClient.dictionarySetFieldsBytesBytes(cacheName, dictionaryName, elements, null);
  }

  /**
   * Gets the field and value from the given dictionary.
   *
   * @param cacheName - The cache containing the dictionary.
   * @param dictionaryName - The dictionary to get the field from.
   * @param field - The field to get.
   * @return Future containing the result of the dictionary get field operation.
   */
  public CompletableFuture<CacheDictionaryGetFieldResponse> dictionaryGetField(
      String cacheName, String dictionaryName, String field) {
    return scsDataClient.dictionaryGetField(cacheName, dictionaryName, field);
  }

  /**
   * Gets the field and value from the given dictionary.
   *
   * @param cacheName - The cache containing the dictionary.
   * @param dictionaryName - The dictionary to get the field from.
   * @param field - The field to get.
   * @return Future containing the result of the dictionary get field operation.
   */
  public CompletableFuture<CacheDictionaryGetFieldResponse> dictionaryGetField(
      String cacheName, String dictionaryName, byte[] field) {
    return scsDataClient.dictionaryGetField(cacheName, dictionaryName, field);
  }

  /**
   * Gets the fields and values from the given dictionary.
   *
   * @param cacheName - The cache containing the dictionary.
   * @param dictionaryName - The dictionary to get the fields and values from.
   * @param fields - The fields to get.
   * @return Future containing the result of the dictionary get fields operation.
   */
  public CompletableFuture<CacheDictionaryGetFieldsResponse> dictionaryGetFields(
      String cacheName, String dictionaryName, List<String> fields) {
    return scsDataClient.dictionaryGetFields(cacheName, dictionaryName, fields);
  }

  /**
   * Gets the fields and values from the given dictionary.
   *
   * @param cacheName - The cache containing the dictionary.
   * @param dictionaryName - The dictionary to get the fields and values from.
   * @param fields - The fields to get.
   * @return Future containing the result of the dictionary get fields operation.
   */
  public CompletableFuture<CacheDictionaryGetFieldsResponse> dictionaryGetFieldsByteArray(
      String cacheName, String dictionaryName, List<byte[]> fields) {
    return scsDataClient.dictionaryGetFieldsByteArray(cacheName, dictionaryName, fields);
  }

  /**
   * Increments the field's value in the given dictionary.
   *
   * @param cacheName - The cache containing the dictionary.
   * @param dictionaryName - The dictionary to increment the value in.
   * @param field - The field for which the value is to be incremented.
   * @return Future containing the result of the dictionary increment operation.
   */
  public CompletableFuture<CacheDictionaryIncrementResponse> dictionaryIncrement(
      String cacheName, String dictionaryName, String field, long amount, CollectionTtl ttl) {
    return scsDataClient.dictionaryIncrement(cacheName, dictionaryName, field, amount, ttl);
  }

  /**
   * Increments the field's value in the given dictionary.
   *
   * @param cacheName - The cache containing the dictionary.
   * @param dictionaryName - The dictionary to increment the value in.
   * @param field - The field for which the value is to be incremented.
   * @return Future containing the result of the dictionary increment operation.
   */
  public CompletableFuture<CacheDictionaryIncrementResponse> dictionaryIncrement(
      String cacheName, String dictionaryName, String field, long amount) {
    return scsDataClient.dictionaryIncrement(cacheName, dictionaryName, field, amount, null);
  }

  /**
   * Increments the field's value in the given dictionary.
   *
   * @param cacheName - The cache containing the dictionary.
   * @param dictionaryName - The dictionary to increment the value in.
   * @param field - The field for which the value is to be incremented.
   * @return Future containing the result of the dictionary increment operation.
   */
  public CompletableFuture<CacheDictionaryIncrementResponse> dictionaryIncrement(
      String cacheName, String dictionaryName, byte[] field, long amount, CollectionTtl ttl) {
    return scsDataClient.dictionaryIncrement(cacheName, dictionaryName, field, amount, ttl);
  }

  /**
   * Increments the field's value in the given dictionary.
   *
   * @param cacheName - The cache containing the dictionary.
   * @param dictionaryName - The dictionary to increment the value in.
   * @param field - The field for which the value is to be incremented.
   * @return Future containing the result of the dictionary increment operation.
   */
  public CompletableFuture<CacheDictionaryIncrementResponse> dictionaryIncrement(
      String cacheName, String dictionaryName, byte[] field, long amount) {
    return scsDataClient.dictionaryIncrement(cacheName, dictionaryName, field, amount, null);
  }

  /**
   * Remove the field from the given dictionary.
   *
   * @param cacheName - The cache containing the dictionary.
   * @param dictionaryName - The dictionary to remove the field from.
   * @param field - The field to remove.
   * @return Future containing the result of the dictionary remove field operation.
   */
  public CompletableFuture<CacheDictionaryRemoveFieldResponse> dictionaryRemoveField(
      String cacheName, String dictionaryName, String field) {
    return scsDataClient.dictionaryRemoveField(cacheName, dictionaryName, field);
  }

  /**
   * Remove the field from the given dictionary.
   *
   * @param cacheName - The cache containing the dictionary.
   * @param dictionaryName - The dictionary to remove the field from.
   * @param field - The field to remove.
   * @return Future containing the result of the dictionary remove field operation.
   */
  public CompletableFuture<CacheDictionaryRemoveFieldResponse> dictionaryRemoveField(
      String cacheName, String dictionaryName, byte[] field) {
    return scsDataClient.dictionaryRemoveField(cacheName, dictionaryName, field);
  }

  /**
   * Removes the fields from the given dictionary.
   *
   * @param cacheName - The cache containing the dictionary.
   * @param dictionaryName - The dictionary to remove the fields from.
   * @param fields - The fields to remove.
   * @return Future containing the result of the dictionary remove fields operation.
   */
  public CompletableFuture<CacheDictionaryRemoveFieldsResponse> dictionaryRemoveFields(
      String cacheName, String dictionaryName, List<String> fields) {
    return scsDataClient.dictionaryRemoveFields(cacheName, dictionaryName, fields);
  }

  /**
   * Removes the fields from the given dictionary.
   *
   * @param cacheName - The cache containing the dictionary.
   * @param dictionaryName - The dictionary to remove the fields from.
   * @param fields - The fields to remove.
   * @return Future containing the result of the dictionary remove fields operation.
   */
  public CompletableFuture<CacheDictionaryRemoveFieldsResponse> dictionaryRemoveFieldsByteArray(
      String cacheName, String dictionaryName, List<byte[]> fields) {
    return scsDataClient.dictionaryRemoveFieldsByteArray(cacheName, dictionaryName, fields);
  }

  @Override
  public void close() {
    scsControlClient.close();
    scsDataClient.close();
  }
}
