package momento.sdk;

import java.io.Closeable;
import java.time.Duration;
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
import momento.sdk.messages.CacheSortedSetGetRankResponse;
import momento.sdk.messages.CacheSortedSetGetScoreResponse;
import momento.sdk.messages.CacheSortedSetGetScoresResponse;
import momento.sdk.messages.CacheSortedSetIncrementScoreResponse;
import momento.sdk.messages.CacheSortedSetPutElementResponse;
import momento.sdk.messages.CacheSortedSetPutElementsResponse;
import momento.sdk.messages.CacheSortedSetRemoveElementResponse;
import momento.sdk.messages.CacheSortedSetRemoveElementsResponse;
import momento.sdk.messages.CreateCacheResponse;
import momento.sdk.messages.CreateSigningKeyResponse;
import momento.sdk.messages.DeleteCacheResponse;
import momento.sdk.messages.FlushCacheResponse;
import momento.sdk.messages.ListCachesResponse;
import momento.sdk.messages.ListSigningKeysResponse;
import momento.sdk.messages.RevokeSigningKeyResponse;
import momento.sdk.messages.ScoredElement;
import momento.sdk.messages.SortOrder;
import momento.sdk.requests.CollectionTtl;

/** Client to perform operations against the Momento Cache Service */
public final class CacheClient implements Closeable {

  private final ScsControlClient scsControlClient;
  private final ScsDataClient scsDataClient;

  /**
   * Constructs a CacheClient.
   *
   * @param credentialProvider Provider for the credentials required to connect to Momento.
   * @param configuration Configuration object containing all tunable client settings.
   * @param itemDefaultTtl The default TTL for values written to a cache.
   */
  public CacheClient(
      @Nonnull CredentialProvider credentialProvider,
      @Nonnull Configuration configuration,
      @Nonnull Duration itemDefaultTtl) {
    this.scsControlClient = new ScsControlClient(credentialProvider);
    this.scsDataClient = new ScsDataClient(credentialProvider, configuration, itemDefaultTtl);
  }

  /**
   * Creates a CacheClient builder.
   *
   * @param credentialProvider Provider for the credentials required to connect to Momento.
   * @param configuration Configuration object containing all tunable client settings.
   * @param itemDefaultTtl The default TTL for values written to a cache.
   * @return The builder.
   */
  public static CacheClientBuilder builder(
      CredentialProvider credentialProvider, Configuration configuration, Duration itemDefaultTtl) {
    return new CacheClientBuilder(credentialProvider, configuration, itemDefaultTtl);
  }

  /**
   * Creates a cache with provided name.
   *
   * @param cacheName The name of the cache to be created.
   * @return A future containing the result of the cache creation: {@link
   *     CreateCacheResponse.Success} or {@link CreateCacheResponse.Error}.
   */
  public CompletableFuture<CreateCacheResponse> createCache(String cacheName) {
    return scsControlClient.createCache(cacheName);
  }

  /**
   * Deletes a cache.
   *
   * @param cacheName The cache to be deleted.
   * @return A future containing the result of the cache deletion: {@link
   *     CacheDeleteResponse.Success} or {@link CacheDeleteResponse.Error}.
   */
  public CompletableFuture<DeleteCacheResponse> deleteCache(String cacheName) {
    return scsControlClient.deleteCache(cacheName);
  }

  /**
   * Flushes the contents of a cache.
   *
   * @param cacheName The cache to be flushed.
   * @return A future containing the result of the cache flush: {@link FlushCacheResponse.Success}
   *     or {@link FlushCacheResponse.Error}.
   */
  public CompletableFuture<FlushCacheResponse> flushCache(String cacheName) {
    return scsControlClient.flushCache(cacheName);
  }

  /**
   * Lists all caches.
   *
   * @return A future containing the result of the list caches operation: {@link
   *     ListCachesResponse.Success} containing the list of caches, or {@link
   *     FlushCacheResponse.Error}.
   */
  public CompletableFuture<ListCachesResponse> listCaches() {
    return scsControlClient.listCaches();
  }

  /**
   * Creates a new Momento signing key.
   *
   * @param ttl The key's time-to-live duration.
   * @return A future containing the result of the signing key creation: {@link
   *     CreateSigningKeyResponse.Success} containing the key and its metadata, or {@link
   *     CreateSigningKeyResponse.Error}.
   */
  public CompletableFuture<CreateSigningKeyResponse> createSigningKey(Duration ttl) {
    return scsControlClient.createSigningKey(ttl);
  }

  /**
   * Revokes a Momento signing key and invalidates all tokens signed by it.
   *
   * @param keyId The ID of the key to revoke.
   * @return A future containing the result of the signing key revocation: {@link
   *     RevokeSigningKeyResponse.Success} or {@link RevokeSigningKeyResponse.Error}.
   */
  public CompletableFuture<RevokeSigningKeyResponse> revokeSigningKey(String keyId) {
    return scsControlClient.revokeSigningKey(keyId);
  }

  /**
   * Lists all Momento signing keys.
   *
   * @return A future containing the result of the signing key revocation: {@link
   *     ListSigningKeysResponse.Success} containing the list of signing keys, or {@link
   *     ListSigningKeysResponse.Error}.
   */
  public CompletableFuture<ListSigningKeysResponse> listSigningKeys() {
    return scsControlClient.listSigningKeys();
  }

  /**
   * Get the cache value stored for the given key.
   *
   * @param cacheName Name of the cache to get the item from.
   * @param key The key to get.
   * @return Future with {@link CacheGetResponse} containing the status of the get operation and the
   *     associated value data.
   */
  public CompletableFuture<CacheGetResponse> get(String cacheName, byte[] key) {
    return scsDataClient.get(cacheName, key);
  }

  /**
   * Get the cache value stored for the given key.
   *
   * @param cacheName Name of the cache to get the item from.
   * @param key The key to get.
   * @return Future with {@link CacheGetResponse} containing the status of the get operation and the
   *     associated value data.
   */
  public CompletableFuture<CacheGetResponse> get(String cacheName, String key) {
    return scsDataClient.get(cacheName, key);
  }

  /**
   * Delete the value stored in Momento cache.
   *
   * @param cacheName Name of the cache to delete the item from.
   * @param key The key to delete
   * @return Future with {@link CacheDeleteResponse}.
   */
  public CompletableFuture<CacheDeleteResponse> delete(String cacheName, String key) {
    return scsDataClient.delete(cacheName, key);
  }

  /**
   * Delete the value stored in Momento cache.
   *
   * @param cacheName Name of the cache to delete the item from.
   * @param key The key to delete.
   * @return Future with {@link CacheDeleteResponse}.
   */
  public CompletableFuture<CacheDeleteResponse> delete(String cacheName, byte[] key) {
    return scsDataClient.delete(cacheName, key);
  }

  /**
   * Sets the value in cache with a given Time To Live (TTL) seconds.
   *
   * <p>If a value for this key is already present it will be replaced by the new value.
   *
   * @param cacheName Name of the cache to store the item in.
   * @param key The key under which the value is to be added.
   * @param value The value to be stored.
   * @param ttl Time to Live for the item in Cache. This TTL takes precedence over the TTL used when
   *     building a cache client {@link CacheClient#builder(CredentialProvider, Configuration,
   *     Duration)}.
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
   * @param cacheName Name of the cache to store the item in.
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
   * @param cacheName Name of the cache to store the item in.
   * @param key The key under which the value is to be added.
   * @param value The value to be stored.
   * @param ttl Time to Live for the item in Cache. This TTL takes precedence over the TTL used when
   *     building a cache client {@link CacheClient#builder(CredentialProvider, Configuration,
   *     Duration)}.
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
   * @param cacheName Name of the cache to store the item in.
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
   * @param cacheName Name of the cache to store the item in.
   * @param key {String} The key under which the value is to be added.
   * @param value {String} The value to be stored.
   * @param ttl Time to Live for the item in Cache. This TTL takes precedence over the TTL used when
   *     building a cache client {@link CacheClient#builder(CredentialProvider, Configuration,
   *     Duration)}.
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
   * @param cacheName Name of the cache to store the item in.
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
   * @param cacheName Name of the cache to store the item in.
   * @param key {String} The key under which the value is to be added.
   * @param value {Byte Array} The value to be stored.
   * @param ttl Time to Live for the item in Cache. This TTL takes precedence over the TTL used when
   *     building a cache client {@link CacheClient#builder(CredentialProvider, Configuration,
   *     Duration)}.
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
   * @param cacheName Name of the cache to store the item in.
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
   * @param cacheName Name of the cache to store the item in.
   * @param key {Byte Array} The key under which the value is to be added.
   * @param value {String} The value to be stored.
   * @param ttl Time to Live for the item in Cache. This TTL takes precedence over the TTL used when
   *     building a cache client {@link CacheClient#builder(CredentialProvider, Configuration,
   *     Duration)}.
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
   * @param cacheName Name of the cache to store the item in.
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
   * @param cacheName Name of the cache to store the item in.
   * @param key {Byte Array} The key under which the value is to be added.
   * @param value {Byte Array} The value to be stored.
   * @param ttl Time to Live for the item in Cache. This TTL takes precedence over the TTL used when
   *     building a cache client {@link CacheClient#builder(CredentialProvider, Configuration,
   *     Duration)}.
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
   * @param cacheName Name of the cache to store the item in.
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
   * @param cacheName Name of the cache to store the item in.
   * @param field The field under which the value is to be added.
   * @param amount The amount by which the cache value is to be incremented.
   * @param ttl Time to Live for the item in Cache. This TTL takes precedence over the TTL used when
   *     building a cache client {@link CacheClient#builder(CredentialProvider, Configuration,
   *     Duration)}.
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
   * @param cacheName Name of the cache to store the item in.
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
   * @param cacheName Name of the cache to store the item in.
   * @param field The field under which the value is to be added.
   * @param amount The amount by which the cache value is to be incremented.
   * @param ttl Time to Live for the item in Cache. This TTL takes precedence over the TTL used when
   *     building a cache client {@link CacheClient#builder(CredentialProvider, Configuration,
   *     Duration)}.
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
   * @param cacheName Name of the cache to store the item in.
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
   * @param cacheName Name of the cache to store the item in.
   * @param setName The set to add the element to.
   * @param element The data to add to the set.
   * @param ttl TTL for the set in cache. This TTL takes precedence over the TTL used when
   *     initializing a cache client. Defaults to client TTL.
   * @return Future containing the result of the add element operation.
   */
  public CompletableFuture<CacheSetAddElementResponse> setAddElement(
      @Nonnull String cacheName,
      @Nonnull String setName,
      @Nonnull String element,
      @Nullable CollectionTtl ttl) {
    return scsDataClient.setAddElement(cacheName, setName, element, ttl);
  }

  /**
   * Add an element to a set in the cache.
   *
   * <p>After this operation the set will contain the union of the element passed in and the
   * original elements of the set.
   *
   * @param cacheName Name of the cache to store the item in.
   * @param setName The set to add the element to.
   * @param element The data to add to the set.
   * @return Future containing the result of the add element operation.
   */
  public CompletableFuture<CacheSetAddElementResponse> setAddElement(
      @Nonnull String cacheName, @Nonnull String setName, @Nonnull String element) {
    return scsDataClient.setAddElement(cacheName, setName, element, null);
  }

  /**
   * Add an element to a set in the cache.
   *
   * <p>After this operation the set will contain the union of the element passed in and the
   * original elements of the set.
   *
   * @param cacheName Name of the cache to store the item in.
   * @param setName The set to add the element to.
   * @param element The data to add to the set.
   * @param ttl TTL for the set in cache. This TTL takes precedence over the TTL used when
   *     initializing a cache client. Defaults to client TTL.
   * @return Future containing the result of the add element operation.
   */
  public CompletableFuture<CacheSetAddElementResponse> setAddElement(
      @Nonnull String cacheName,
      @Nonnull String setName,
      @Nonnull byte[] element,
      @Nullable CollectionTtl ttl) {
    return scsDataClient.setAddElement(cacheName, setName, element, ttl);
  }

  /**
   * Add an element to a set in the cache.
   *
   * <p>After this operation the set will contain the union of the element passed in and the
   * original elements of the set.
   *
   * @param cacheName Name of the cache to store the item in.
   * @param setName The set to add the element to.
   * @param element The data to add to the set.
   * @return Future containing the result of the add element operation.
   */
  public CompletableFuture<CacheSetAddElementResponse> setAddElement(
      @Nonnull String cacheName, @Nonnull String setName, @Nonnull byte[] element) {
    return scsDataClient.setAddElement(cacheName, setName, element, null);
  }

  /**
   * Add several string elements to a set in the cache.
   *
   * <p>After this operation the set will contain the union of the elements passed in and the
   * original elements of the set.
   *
   * @param cacheName Name of the cache to store the item in.
   * @param setName The set to add the elements to.
   * @param elements The data to add to the set.
   * @param ttl TTL for the set in cache. This TTL takes precedence over the TTL used when
   *     initializing a cache client. Defaults to client TTL.
   * @return Future containing the result of the add elements operation.
   */
  public CompletableFuture<CacheSetAddElementsResponse> setAddElements(
      @Nonnull String cacheName,
      @Nonnull String setName,
      @Nonnull Set<String> elements,
      @Nullable CollectionTtl ttl) {
    return scsDataClient.setAddElements(cacheName, setName, elements, ttl);
  }

  /**
   * Add several string elements to a set in the cache.
   *
   * <p>After this operation the set will contain the union of the elements passed in and the
   * original elements of the set.
   *
   * @param cacheName Name of the cache to store the item in.
   * @param setName The set to add the elements to.
   * @param elements The data to add to the set.
   * @return Future containing the result of the add elements operation.
   */
  public CompletableFuture<CacheSetAddElementsResponse> setAddElements(
      @Nonnull String cacheName, @Nonnull String setName, @Nonnull Set<String> elements) {
    return scsDataClient.setAddElements(cacheName, setName, elements, null);
  }

  /**
   * Add several byte array elements to a set in the cache.
   *
   * <p>After this operation the set will contain the union of the elements passed in and the
   * original elements of the set.
   *
   * @param cacheName Name of the cache to store the item in.
   * @param setName The set to add the elements to.
   * @param elements The data to add to the set.
   * @param ttl TTL for the set in cache. This TTL takes precedence over the TTL used when
   *     initializing a cache client. Defaults to client TTL.
   * @return Future containing the result of the add elements operation.
   */
  public CompletableFuture<CacheSetAddElementsResponse> setAddElementsByteArray(
      @Nonnull String cacheName,
      @Nonnull String setName,
      @Nonnull Set<byte[]> elements,
      @Nullable CollectionTtl ttl) {
    return scsDataClient.setAddElementsByteArray(cacheName, setName, elements, ttl);
  }

  /**
   * Add several byte array elements to a set in the cache.
   *
   * <p>After this operation the set will contain the union of the elements passed in and the
   * original elements of the set.
   *
   * @param cacheName Name of the cache to store the item in.
   * @param setName The set to add the elements to.
   * @param elements The data to add to the set.
   * @return Future containing the result of the add elements operation.
   */
  public CompletableFuture<CacheSetAddElementsResponse> setAddElementsByteArray(
      @Nonnull String cacheName, @Nonnull String setName, @Nonnull Set<byte[]> elements) {
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
      @Nonnull String cacheName, @Nonnull String setName, @Nonnull String element) {
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
      @Nonnull String cacheName, @Nonnull String setName, @Nonnull byte[] element) {
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
      @Nonnull String cacheName, @Nonnull String setName, @Nonnull Set<String> elements) {
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
      @Nonnull String cacheName, @Nonnull String setName, @Nonnull Set<byte[]> elements) {
    return scsDataClient.setRemoveElementsByteArray(cacheName, setName, elements);
  }

  /**
   * Fetch an entire set from the cache.
   *
   * @param cacheName Name of the cache to perform the lookup in.
   * @param setName The set to fetch.
   * @return Future containing the result of the fetch operation.
   */
  public CompletableFuture<CacheSetFetchResponse> setFetch(
      @Nonnull String cacheName, @Nonnull String setName) {
    return scsDataClient.setFetch(cacheName, setName);
  }

  /**
   * Adds an element to the given sorted set. If the element already exists, its score is updated.
   * Creates the sorted set if it does not exist.
   *
   * @param cacheName - The cache containing the sorted set.
   * @param sortedSetName - The sorted set to add to.
   * @param value - The value of the element to add.
   * @param score - The score to assign to the element.
   * @param ttl TTL for the set in cache. This TTL takes precedence over the TTL used when
   *     initializing a cache client. Defaults to client TTL.
   * @return Future containing the result of the put element operation.
   */
  public CompletableFuture<CacheSortedSetPutElementResponse> sortedSetPutElement(
      @Nonnull String cacheName,
      @Nonnull String sortedSetName,
      @Nonnull String value,
      double score,
      @Nullable CollectionTtl ttl) {
    return scsDataClient.sortedSetPutElement(cacheName, sortedSetName, value, score, ttl);
  }

  /**
   * Adds an element to the given sorted set. If the element already exists, its score is updated.
   * Creates the sorted set if it does not exist.
   *
   * @param cacheName - The cache containing the sorted set.
   * @param sortedSetName - The sorted set to add to.
   * @param value - The value of the element to add.
   * @param score - The score to assign to the element.
   * @return Future containing the result of the put element operation.
   */
  public CompletableFuture<CacheSortedSetPutElementResponse> sortedSetPutElement(
      @Nonnull String cacheName,
      @Nonnull String sortedSetName,
      @Nonnull String value,
      double score) {
    return scsDataClient.sortedSetPutElement(cacheName, sortedSetName, value, score, null);
  }

  /**
   * Adds an element to the given sorted set. If the element already exists, its score is updated.
   * Creates the sorted set if it does not exist.
   *
   * @param cacheName - The cache containing the sorted set.
   * @param sortedSetName - The sorted set to add to.
   * @param value - The value of the element to add.
   * @param score - The score to assign to the element.
   * @param ttl TTL for the set in cache. This TTL takes precedence over the TTL used when
   *     initializing a cache client. Defaults to client TTL.
   * @return Future containing the result of the put element operation.
   */
  public CompletableFuture<CacheSortedSetPutElementResponse> sortedSetPutElement(
      @Nonnull String cacheName,
      @Nonnull String sortedSetName,
      @Nonnull byte[] value,
      double score,
      @Nullable CollectionTtl ttl) {
    return scsDataClient.sortedSetPutElement(cacheName, sortedSetName, value, score, ttl);
  }

  /**
   * Adds an element to the given sorted set. If the element already exists, its score is updated.
   * Creates the sorted set if it does not exist.
   *
   * @param cacheName - The cache containing the sorted set.
   * @param sortedSetName - The sorted set to add to.
   * @param value - The value of the element to add.
   * @param score - The score to assign to the element.
   * @return Future containing the result of the put element operation.
   */
  public CompletableFuture<CacheSortedSetPutElementResponse> sortedSetPutElement(
      @Nonnull String cacheName,
      @Nonnull String sortedSetName,
      @Nonnull byte[] value,
      double score) {
    return scsDataClient.sortedSetPutElement(cacheName, sortedSetName, value, score, null);
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
      @Nonnull String cacheName,
      @Nonnull String sortedSetName,
      @Nonnull Map<String, Double> elements,
      @Nullable CollectionTtl ttl) {
    return scsDataClient.sortedSetPutElements(cacheName, sortedSetName, elements, ttl);
  }

  /**
   * Adds elements to the given sorted set. If the elements already exist, their scores are updated.
   * Creates the sorted set if it does not exist.
   *
   * @param cacheName - The cache containing the sorted set.
   * @param sortedSetName - The sorted set to add to.
   * @param elements - The value to score map to add to the sorted set.
   * @return Future containing the result of the put elements operation.
   */
  public CompletableFuture<CacheSortedSetPutElementsResponse> sortedSetPutElements(
      @Nonnull String cacheName,
      @Nonnull String sortedSetName,
      @Nonnull Map<String, Double> elements) {
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
      @Nonnull String cacheName,
      @Nonnull String sortedSetName,
      @Nonnull Map<byte[], Double> elements,
      @Nullable CollectionTtl ttl) {
    return scsDataClient.sortedSetPutElementsByteArray(cacheName, sortedSetName, elements, ttl);
  }

  /**
   * Adds elements to the given sorted set. If the elements already exist, their scores are updated.
   * Creates the sorted set if it does not exist.
   *
   * @param cacheName - The cache containing the sorted set.
   * @param sortedSetName - The sorted set to add to.
   * @param elements - The value to score map to add to the sorted set.
   * @return Future containing the result of the put elements operation.
   */
  public CompletableFuture<CacheSortedSetPutElementsResponse> sortedSetPutElementsByteArray(
      @Nonnull String cacheName,
      @Nonnull String sortedSetName,
      @Nonnull Map<byte[], Double> elements) {
    return scsDataClient.sortedSetPutElementsByteArray(cacheName, sortedSetName, elements, null);
  }

  /**
   * Adds elements to the given sorted set. If the elements already exist, their scores are updated.
   * Creates the sorted set if it does not exist.
   *
   * @param cacheName - The cache containing the sorted set.
   * @param sortedSetName - The sorted set to add to.
   * @param elements - The value-score pairs to add to the sorted set.
   * @param ttl TTL for the set in cache. This TTL takes precedence over the TTL used when
   *     initializing a cache client. Defaults to client TTL.
   * @return Future containing the result of the put elements operation.
   */
  public CompletableFuture<CacheSortedSetPutElementsResponse> sortedSetPutElements(
      @Nonnull String cacheName,
      @Nonnull String sortedSetName,
      @Nonnull Iterable<ScoredElement> elements,
      @Nullable CollectionTtl ttl) {
    return scsDataClient.sortedSetPutElements(cacheName, sortedSetName, elements, ttl);
  }

  /**
   * Adds elements to the given sorted set. If the elements already exist, their scores are updated.
   * * Creates the sorted set if it does not exist.
   *
   * @param cacheName - The cache containing the sorted set.
   * @param sortedSetName - The sorted set to add to.
   * @param elements - The value-score pairs to add to the sorted set.
   * @return Future containing the result of the put elements operation.
   */
  public CompletableFuture<CacheSortedSetPutElementsResponse> sortedSetPutElements(
      @Nonnull String cacheName,
      @Nonnull String sortedSetName,
      @Nonnull Iterable<ScoredElement> elements) {
    return scsDataClient.sortedSetPutElements(cacheName, sortedSetName, elements, null);
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
   * @param order - The order to fetch the elements in. Defaults to ascending.
   * @return Future containing the result of the fetch operation.
   */
  public CompletableFuture<CacheSortedSetFetchResponse> sortedSetFetchByRank(
      @Nonnull String cacheName,
      @Nonnull String sortedSetName,
      @Nullable Integer startRank,
      @Nullable Integer endRank,
      @Nullable SortOrder order) {
    return scsDataClient.sortedSetFetchByRank(cacheName, sortedSetName, startRank, endRank, order);
  }

  /**
   * Fetch all elements in the given sorted set by index (rank) in ascending order.
   *
   * @param cacheName - The cache containing the sorted set.
   * @param sortedSetName - The sorted set to fetch from.
   * @return Future containing the result of the fetch operation.
   */
  public CompletableFuture<CacheSortedSetFetchResponse> sortedSetFetchByRank(
      @Nonnull String cacheName, @Nonnull String sortedSetName) {
    return scsDataClient.sortedSetFetchByRank(cacheName, sortedSetName, null, null, null);
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
   * @param order - The order to fetch the elements in. Defaults to ascending.
   * @param offset - The number of elements to skip before returning the first element. Defaults to
   *     0. Note: this is not the rank of the first element to return, but the number of elements of
   *     the result set to skip before returning the first element.
   * @param count - The maximum number of elements to return. Defaults to all elements.
   * @return Future containing the result of the fetch operation.
   */
  public CompletableFuture<CacheSortedSetFetchResponse> sortedSetFetchByScore(
      @Nonnull String cacheName,
      @Nonnull String sortedSetName,
      @Nullable Double minScore,
      @Nullable Double maxScore,
      @Nullable SortOrder order,
      @Nullable Integer offset,
      @Nullable Integer count) {
    return scsDataClient.sortedSetFetchByScore(
        cacheName, sortedSetName, minScore, maxScore, order, offset, count);
  }

  /**
   * Fetch all elements in the given sorted set by score in ascending order.
   *
   * @param cacheName - The cache containing the sorted set.
   * @param sortedSetName - The sorted set to fetch from.
   * @param minScore - The minimum score (inclusive) of the elements to fetch. Defaults to negative
   *     infinity.
   * @param maxScore - The maximum score (inclusive) of the elements to fetch. Defaults to positive
   *     infinity.
   * @param order - The order to fetch the elements in. Defaults to ascending.
   * @return Future containing the result of the fetch operation.
   */
  public CompletableFuture<CacheSortedSetFetchResponse> sortedSetFetchByScore(
      @Nonnull String cacheName,
      @Nonnull String sortedSetName,
      @Nullable Double minScore,
      @Nullable Double maxScore,
      @Nullable SortOrder order) {
    return scsDataClient.sortedSetFetchByScore(
        cacheName, sortedSetName, minScore, maxScore, order, null, null);
  }

  /**
   * Fetch all elements in the given sorted set by score in ascending order.
   *
   * @param cacheName - The cache containing the sorted set.
   * @param sortedSetName - The sorted set to fetch from.
   * @return Future containing the result of the fetch operation.
   */
  public CompletableFuture<CacheSortedSetFetchResponse> sortedSetFetchByScore(
      @Nonnull String cacheName, @Nonnull String sortedSetName) {
    return scsDataClient.sortedSetFetchByScore(
        cacheName, sortedSetName, null, null, null, null, null);
  }

  /**
   * Look up the rank of an element in a sorted set.
   *
   * @param cacheName - The cache containing the sorted set.
   * @param sortedSetName - The sorted set to fetch from.
   * @param value - The value of element whose rank we are retrieving.
   * @param order - The order to read through the scores of the set. Affects the rank. Defaults to
   *     ascending, i.e. the rank of the element with the lowest score will be 0.
   * @return Future containing the result of the get rank operation.
   */
  public CompletableFuture<CacheSortedSetGetRankResponse> sortedSetGetRank(
      @Nonnull String cacheName,
      @Nonnull String sortedSetName,
      @Nonnull String value,
      @Nullable SortOrder order) {
    return scsDataClient.sortedSetGetRank(cacheName, sortedSetName, value, order);
  }

  /**
   * Look up the rank of an element in a sorted set.
   *
   * @param cacheName - The cache containing the sorted set.
   * @param sortedSetName - The sorted set to fetch from.
   * @param value - The value of element whose rank we are retrieving.
   * @param order - The order to read through the scores of the set. Affects the rank. Defaults to
   *     ascending, i.e. the rank of the element with the lowest score will be 0.
   * @return Future containing the result of the get rank operation.
   */
  public CompletableFuture<CacheSortedSetGetRankResponse> sortedSetGetRank(
      @Nonnull String cacheName,
      @Nonnull String sortedSetName,
      @Nonnull byte[] value,
      @Nullable SortOrder order) {
    return scsDataClient.sortedSetGetRank(cacheName, sortedSetName, value, order);
  }

  /**
   * Look up the score of an element in a sorted set.
   *
   * @param cacheName - The cache containing the sorted set.
   * @param sortedSetName - The sorted set to fetch from.
   * @param value - The value whose score we are retrieving.
   * @return Future containing the result of the get score operation.
   */
  public CompletableFuture<CacheSortedSetGetScoreResponse> sortedSetGetScore(
      @Nonnull String cacheName, @Nonnull String sortedSetName, @Nonnull String value) {
    return scsDataClient.sortedSetGetScore(cacheName, sortedSetName, value);
  }

  /**
   * Look up the score of an element in a sorted set.
   *
   * @param cacheName - The cache containing the sorted set.
   * @param sortedSetName - The sorted set to fetch from.
   * @param value - The value whose score we are retrieving.
   * @return Future containing the result of the get score operation.
   */
  public CompletableFuture<CacheSortedSetGetScoreResponse> sortedSetGetScore(
      @Nonnull String cacheName, @Nonnull String sortedSetName, @Nonnull byte[] value) {
    return scsDataClient.sortedSetGetScore(cacheName, sortedSetName, value);
  }

  /**
   * Look up the scores of elements in a sorted set.
   *
   * @param cacheName - The cache containing the sorted set.
   * @param sortedSetName - The sorted set to fetch from.
   * @param values - The values whose scores we are retrieving.
   * @return Future containing the result of the get scores operation.
   */
  public CompletableFuture<CacheSortedSetGetScoresResponse> sortedSetGetScores(
      @Nonnull String cacheName, @Nonnull String sortedSetName, @Nonnull Iterable<String> values) {
    return scsDataClient.sortedSetGetScores(cacheName, sortedSetName, values);
  }

  /**
   * Look up the scores of elements in a sorted set.
   *
   * @param cacheName - The cache containing the sorted set.
   * @param sortedSetName - The sorted set to fetch from.
   * @param values - The values whose scores we are retrieving.
   * @return Future containing the result of the get scores operation.
   */
  public CompletableFuture<CacheSortedSetGetScoresResponse> sortedSetGetScoresByteArray(
      @Nonnull String cacheName, @Nonnull String sortedSetName, @Nonnull Iterable<byte[]> values) {
    return scsDataClient.sortedSetGetScoresByteArray(cacheName, sortedSetName, values);
  }

  /**
   * Increment the score of an element in a sorted set.
   *
   * @param cacheName - The cache containing the sorted set.
   * @param sortedSetName - The sorted set to fetch from.
   * @param value - The value whose score we are incrementing.
   * @param amount - The quantity to add to the score. May be positive, negative, or zero.
   * @param ttl TTL for the set in cache. This TTL takes precedence over the TTL used when
   *     initializing a cache client. Defaults to client TTL.
   * @return Future containing the result of the increment operation.
   */
  public CompletableFuture<CacheSortedSetIncrementScoreResponse> sortedSetIncrementScore(
      @Nonnull String cacheName,
      @Nonnull String sortedSetName,
      @Nonnull String value,
      double amount,
      @Nullable CollectionTtl ttl) {
    return scsDataClient.sortedSetIncrementScore(cacheName, sortedSetName, value, amount, ttl);
  }

  /**
   * Increment the score of an element in a sorted set.
   *
   * @param cacheName - The cache containing the sorted set.
   * @param sortedSetName - The sorted set to fetch from.
   * @param value - The value whose score we are incrementing.
   * @param amount - The quantity to add to the score. May be positive, negative, or zero.
   * @return Future containing the result of the increment operation.
   */
  public CompletableFuture<CacheSortedSetIncrementScoreResponse> sortedSetIncrementScore(
      @Nonnull String cacheName,
      @Nonnull String sortedSetName,
      @Nonnull String value,
      double amount) {
    return scsDataClient.sortedSetIncrementScore(cacheName, sortedSetName, value, amount, null);
  }

  /**
   * Increment the score of an element in a sorted set.
   *
   * @param cacheName - The cache containing the sorted set.
   * @param sortedSetName - The sorted set to fetch from.
   * @param value - The value whose score we are incrementing.
   * @param amount - The quantity to add to the score. May be positive, negative, or zero.
   * @param ttl TTL for the set in cache. This TTL takes precedence over the TTL used when
   *     initializing a cache client. Defaults to client TTL.
   * @return Future containing the result of the increment operation.
   */
  public CompletableFuture<CacheSortedSetIncrementScoreResponse> sortedSetIncrementScore(
      @Nonnull String cacheName,
      @Nonnull String sortedSetName,
      @Nonnull byte[] value,
      double amount,
      @Nullable CollectionTtl ttl) {
    return scsDataClient.sortedSetIncrementScore(cacheName, sortedSetName, value, amount, ttl);
  }

  /**
   * Increment the score of an element in a sorted set.
   *
   * @param cacheName - The cache containing the sorted set.
   * @param sortedSetName - The sorted set to fetch from.
   * @param value - The value whose score we are incrementing.
   * @param amount - The quantity to add to the score. May be positive, negative, or zero.
   * @return Future containing the result of the increment operation.
   */
  public CompletableFuture<CacheSortedSetIncrementScoreResponse> sortedSetIncrementScore(
      @Nonnull String cacheName,
      @Nonnull String sortedSetName,
      @Nonnull byte[] value,
      double amount) {
    return scsDataClient.sortedSetIncrementScore(cacheName, sortedSetName, value, amount, null);
  }

  /**
   * Remove an element from a sorted set
   *
   * @param cacheName - The cache containing the sorted set.
   * @param sortedSetName - The sorted set to remove from.
   * @param value - The value of the element to remove from the set.
   * @return Future containing the result of the remove operation.
   */
  public CompletableFuture<CacheSortedSetRemoveElementResponse> sortedSetRemoveElement(
      @Nonnull String cacheName, @Nonnull String sortedSetName, @Nonnull String value) {
    return scsDataClient.sortedSetRemoveElement(cacheName, sortedSetName, value);
  }

  /**
   * Remove an element from a sorted set
   *
   * @param cacheName - The cache containing the sorted set.
   * @param sortedSetName - The sorted set to remove from.
   * @param value - The value of the element to remove from the set.
   * @return Future containing the result of the remove operation.
   */
  public CompletableFuture<CacheSortedSetRemoveElementResponse> sortedSetRemoveElement(
      @Nonnull String cacheName, @Nonnull String sortedSetName, @Nonnull byte[] value) {
    return scsDataClient.sortedSetRemoveElement(cacheName, sortedSetName, value);
  }

  /**
   * Remove elements from a sorted set
   *
   * @param cacheName - The cache containing the sorted set.
   * @param sortedSetName - The sorted set to remove from.
   * @param values - The values of elements to remove from the set.
   * @return Future containing the result of the remove operation.
   */
  public CompletableFuture<CacheSortedSetRemoveElementsResponse> sortedSetRemoveElements(
      @Nonnull String cacheName, @Nonnull String sortedSetName, @Nonnull Iterable<String> values) {
    return scsDataClient.sortedSetRemoveElements(cacheName, sortedSetName, values);
  }

  /**
   * Remove elements from a sorted set
   *
   * @param cacheName - The cache containing the sorted set.
   * @param sortedSetName - The sorted set to remove from.
   * @param values - The values of the elements to remove from the set.
   * @return Future containing the result of the remove operation.
   */
  public CompletableFuture<CacheSortedSetRemoveElementsResponse> sortedSetRemoveElementsByteArray(
      @Nonnull String cacheName, @Nonnull String sortedSetName, @Nonnull Iterable<byte[]> values) {
    return scsDataClient.sortedSetRemoveElementsByteArray(cacheName, sortedSetName, values);
  }

  /**
   * Adds the given values to the back of a list.
   *
   * @param cacheName The cache containing the list.
   * @param listName The list in which to add the values.
   * @param values The values to add to the list.
   * @param truncateFrontToSize If the list exceeds this length, remove excess from the front of the
   *     list. Must be positive. Will not truncate if not provided.
   * @param ttl TTL for the set in cache. This TTL takes precedence over the TTL used when
   *     initializing a cache client. Defaults to the client's TTL if not provided.
   * @return Future containing the result of the list concatenate back operation: {@link
   *     CacheListConcatenateBackResponse.Success} or {@link
   *     CacheListConcatenateBackResponse.Error}.
   */
  public CompletableFuture<CacheListConcatenateBackResponse> listConcatenateBack(
      @Nonnull String cacheName,
      @Nonnull String listName,
      @Nonnull Iterable<String> values,
      @Nullable Integer truncateFrontToSize,
      @Nullable CollectionTtl ttl) {
    return scsDataClient.listConcatenateBack(cacheName, listName, values, truncateFrontToSize, ttl);
  }

  /**
   * Adds the given values to the back of a list. Refreshes the list with the client's default TTL.
   *
   * @param cacheName The cache containing the list.
   * @param listName The list in which to add the values.
   * @param values The values to add to the list.
   * @param truncateFrontToSize If the list exceeds this length, remove excess from the front of the
   *     list. Must be positive. Will not truncate if not provided.
   * @return Future containing the result of the list concatenate back operation: {@link
   *     CacheListConcatenateBackResponse.Success} or {@link
   *     CacheListConcatenateBackResponse.Error}.
   */
  public CompletableFuture<CacheListConcatenateBackResponse> listConcatenateBack(
      @Nonnull String cacheName,
      @Nonnull String listName,
      @Nonnull Iterable<String> values,
      @Nullable Integer truncateFrontToSize) {
    return scsDataClient.listConcatenateBack(
        cacheName, listName, values, truncateFrontToSize, null);
  }

  /**
   * Adds the given values to the back of a list. Refreshes the list with the client's default TTL.
   *
   * @param cacheName The cache containing the list.
   * @param listName The list in which to add the values.
   * @param values The values to add to the list.
   * @return Future containing the result of the list concatenate back operation: {@link
   *     CacheListConcatenateBackResponse.Success} or {@link
   *     CacheListConcatenateBackResponse.Error}.
   */
  public CompletableFuture<CacheListConcatenateBackResponse> listConcatenateBack(
      @Nonnull String cacheName, @Nonnull String listName, @Nonnull Iterable<String> values) {
    return scsDataClient.listConcatenateBack(cacheName, listName, values, null, null);
  }

  /**
   * Adds the given values to the back of a list.
   *
   * @param cacheName The cache containing the list.
   * @param listName The list in which to add the values.
   * @param values The values to add to the list.
   * @param truncateFrontToSize If the list exceeds this length, remove excess from the front of the
   *     list. Must be positive. Will not truncate if not provided.
   * @param ttl TTL for the set in cache. This TTL takes precedence over the TTL used when
   *     initializing a cache client. Defaults to the client's TTL if not provided.
   * @return Future containing the result of the list concatenate back operation: {@link
   *     CacheListConcatenateBackResponse.Success} or {@link
   *     CacheListConcatenateBackResponse.Error}.
   */
  public CompletableFuture<CacheListConcatenateBackResponse> listConcatenateBackByteArray(
      @Nonnull String cacheName,
      @Nonnull String listName,
      @Nonnull Iterable<byte[]> values,
      @Nullable Integer truncateFrontToSize,
      @Nullable CollectionTtl ttl) {
    return scsDataClient.listConcatenateBackByteArray(
        cacheName, listName, values, truncateFrontToSize, ttl);
  }

  /**
   * Adds the given values to the back of a list. Refreshes the list with the client's default TTL.
   *
   * @param cacheName The cache containing the list.
   * @param listName The list in which to add the values.
   * @param values The values to add to the list.
   * @param truncateFrontToSize If the list exceeds this length, remove excess from the front of the
   *     list. Must be positive. Will not truncate if not provided.
   * @return Future containing the result of the list concatenate back operation: {@link
   *     CacheListConcatenateBackResponse.Success} or {@link
   *     CacheListConcatenateBackResponse.Error}.
   */
  public CompletableFuture<CacheListConcatenateBackResponse> listConcatenateBackByteArray(
      @Nonnull String cacheName,
      @Nonnull String listName,
      @Nonnull Iterable<byte[]> values,
      @Nullable Integer truncateFrontToSize) {
    return scsDataClient.listConcatenateBackByteArray(
        cacheName, listName, values, truncateFrontToSize, null);
  }

  /**
   * Adds the given values to the back of a list. Refreshes the list with the client's default TTL.
   *
   * @param cacheName The cache containing the list.
   * @param listName The list in which to add the values.
   * @param values The values to add to the list.
   * @return Future containing the result of the list concatenate back operation: {@link
   *     CacheListConcatenateBackResponse.Success} or {@link
   *     CacheListConcatenateBackResponse.Error}.
   */
  public CompletableFuture<CacheListConcatenateBackResponse> listConcatenateBackByteArray(
      @Nonnull String cacheName, @Nonnull String listName, @Nonnull Iterable<byte[]> values) {
    return scsDataClient.listConcatenateBackByteArray(cacheName, listName, values, null, null);
  }

  /**
   * Adds the given values to the front of a list.
   *
   * @param cacheName The cache containing the list.
   * @param listName The list in which to add the values.
   * @param values The values to add to the list.
   * @param truncateBackToSize If the list exceeds this length, remove excess from the front of the
   *     list. Must be positive. Will not truncate if not provided.
   * @param ttl TTL for the set in cache. This TTL takes precedence over the TTL used when
   *     initializing a cache client. Defaults to the client's TTL if not provided.
   * @return Future containing the result of the list concatenate front operation: {@link
   *     CacheListConcatenateFrontResponse.Success} or {@link
   *     CacheListConcatenateFrontResponse.Error}.
   */
  public CompletableFuture<CacheListConcatenateFrontResponse> listConcatenateFront(
      @Nonnull String cacheName,
      @Nonnull String listName,
      @Nonnull Iterable<String> values,
      @Nullable Integer truncateBackToSize,
      @Nullable CollectionTtl ttl) {
    return scsDataClient.listConcatenateFront(cacheName, listName, values, truncateBackToSize, ttl);
  }

  /**
   * Adds the given values to the front of a list. Refreshes the list with the client's default TTL.
   *
   * @param cacheName The cache containing the list.
   * @param listName The list in which to add the values.
   * @param values The values to add to the list.
   * @param truncateBackToSize If the list exceeds this length, remove excess from the front of the
   *     list. Must be positive. Will not truncate if not provided.
   * @return Future containing the result of the list concatenate front operation: {@link
   *     CacheListConcatenateFrontResponse.Success} or {@link
   *     CacheListConcatenateFrontResponse.Error}.
   */
  public CompletableFuture<CacheListConcatenateFrontResponse> listConcatenateFront(
      @Nonnull String cacheName,
      @Nonnull String listName,
      @Nonnull Iterable<String> values,
      @Nullable Integer truncateBackToSize) {
    return scsDataClient.listConcatenateFront(
        cacheName, listName, values, truncateBackToSize, null);
  }

  /**
   * Adds the given values to the front of a list. Refreshes the list with the client's default TTL.
   *
   * @param cacheName The cache containing the list.
   * @param listName The list in which to add the values.
   * @param values The values to add to the list.
   * @return Future containing the result of the list concatenate front operation: {@link
   *     CacheListConcatenateFrontResponse.Success} or {@link
   *     CacheListConcatenateFrontResponse.Error}.
   */
  public CompletableFuture<CacheListConcatenateFrontResponse> listConcatenateFront(
      @Nonnull String cacheName, @Nonnull String listName, @Nonnull Iterable<String> values) {
    return scsDataClient.listConcatenateFront(cacheName, listName, values, null, null);
  }

  /**
   * Adds the given values to the front of a list.
   *
   * @param cacheName The cache containing the list.
   * @param listName The list in which to add the values.
   * @param values The values to add to the list.
   * @param truncateBackToSize If the list exceeds this length, remove excess from the front of the
   *     list. Must be positive. Will not truncate if not provided.
   * @param ttl TTL for the set in cache. This TTL takes precedence over the TTL used when
   *     initializing a cache client. Defaults to the client's TTL if not provided.
   * @return Future containing the result of the list concatenate front operation: {@link
   *     CacheListConcatenateFrontResponse.Success} or {@link
   *     CacheListConcatenateFrontResponse.Error}.
   */
  public CompletableFuture<CacheListConcatenateFrontResponse> listConcatenateFrontByteArray(
      @Nonnull String cacheName,
      @Nonnull String listName,
      @Nonnull Iterable<byte[]> values,
      @Nullable Integer truncateBackToSize,
      @Nullable CollectionTtl ttl) {
    return scsDataClient.listConcatenateFrontByteArray(
        cacheName, listName, values, truncateBackToSize, ttl);
  }

  /**
   * Adds the given values to the front of a list. Refreshes the list with the client's default TTL.
   *
   * @param cacheName The cache containing the list.
   * @param listName The list in which to add the values.
   * @param values The values to add to the list.
   * @param truncateBackToSize If the list exceeds this length, remove excess from the front of the
   *     list. Must be positive. Will not truncate if not provided.
   * @return Future containing the result of the list concatenate front operation: {@link
   *     CacheListConcatenateFrontResponse.Success} or {@link
   *     CacheListConcatenateFrontResponse.Error}.
   */
  public CompletableFuture<CacheListConcatenateFrontResponse> listConcatenateFrontByteArray(
      @Nonnull String cacheName,
      @Nonnull String listName,
      @Nonnull Iterable<byte[]> values,
      @Nullable Integer truncateBackToSize) {
    return scsDataClient.listConcatenateFrontByteArray(
        cacheName, listName, values, truncateBackToSize, null);
  }

  /**
   * Adds the given values to the front of a list. Refreshes the list with the client's default TTL.
   *
   * @param cacheName The cache containing the list.
   * @param listName The list in which to add the values.
   * @param values The values to add to the list.
   * @return Future containing the result of the list concatenate front operation: {@link
   *     CacheListConcatenateFrontResponse.Success} or {@link
   *     CacheListConcatenateFrontResponse.Error}.
   */
  public CompletableFuture<CacheListConcatenateFrontResponse> listConcatenateFrontByteArray(
      @Nonnull String cacheName, @Nonnull String listName, @Nonnull Iterable<byte[]> values) {
    return scsDataClient.listConcatenateFrontByteArray(cacheName, listName, values, null, null);
  }

  /**
   * Fetches all elements of a list between the given indices.
   *
   * @param cacheName The cache containing the list.
   * @param listName The list to fetch.
   * @param startIndex Start index (inclusive) for fetch operation. Defaults to 0 if not provided.
   * @param endIndex End index (inclusive) for fetch operation. Defaults to the end of the list if
   *     not provided.
   * @return Future containing the result of the list fetch back operation: {@link
   *     CacheListFetchResponse.Hit} containing the fetched data, {@link
   *     CacheListFetchResponse.Miss} if no data was found, or {@link CacheListFetchResponse.Error}.
   */
  public CompletableFuture<CacheListFetchResponse> listFetch(
      @Nonnull String cacheName,
      @Nonnull String listName,
      @Nullable Integer startIndex,
      @Nullable Integer endIndex) {
    return scsDataClient.listFetch(cacheName, listName, startIndex, endIndex);
  }

  /**
   * Fetches all elements of a list between the given indices.
   *
   * @param cacheName The cache containing the list.
   * @param listName The list to fetch.
   * @return Future containing the result of the list fetch back operation: {@link
   *     CacheListFetchResponse.Hit} containing the fetched data, {@link
   *     CacheListFetchResponse.Miss} if no data was found, or {@link CacheListFetchResponse.Error}.
   */
  public CompletableFuture<CacheListFetchResponse> listFetch(
      @Nonnull String cacheName, @Nonnull String listName) {
    return scsDataClient.listFetch(cacheName, listName, null, null);
  }

  /**
   * Fetches the length of a list.
   *
   * @param cacheName The cache containing the list.
   * @param listName The list to measure.
   * @return Future containing the result of the list length back operation: {@link
   *     CacheListLengthResponse.Hit} containing the length, {@link CacheListLengthResponse.Miss} if
   *     no list was found, or {@link CacheListLengthResponse.Error}.
   */
  public CompletableFuture<CacheListLengthResponse> listLength(
      @Nonnull String cacheName, @Nonnull String listName) {
    return scsDataClient.listLength(cacheName, listName);
  }

  /**
   * Fetches and removes the element from the back of a list.
   *
   * @param cacheName The cache containing the list.
   * @param listName The list to fetch the value from.
   * @return Future containing the result of the list pop back operation: {@link
   *     CacheListPopBackResponse.Hit} containing the element, {@link CacheListPopBackResponse.Miss}
   *     if no list was found, or {@link CacheListPopBackResponse.Error}.
   */
  public CompletableFuture<CacheListPopBackResponse> listPopBack(
      @Nonnull String cacheName, @Nonnull String listName) {
    return scsDataClient.listPopBack(cacheName, listName);
  }

  /**
   * Fetches and removes the element from the front of a list.
   *
   * @param cacheName The cache containing the list.
   * @param listName The list to fetch the value from.
   * @return Future containing the result of the list pop front operation: {@link
   *     CacheListPopFrontResponse.Hit} containing the element, {@link
   *     CacheListPopFrontResponse.Miss} if no list was found, or {@link
   *     CacheListPopFrontResponse.Error}.
   */
  public CompletableFuture<CacheListPopFrontResponse> listPopFront(
      @Nonnull String cacheName, @Nonnull String listName) {
    return scsDataClient.listPopFront(cacheName, listName);
  }

  /**
   * Pushes a value to the back of a list.
   *
   * @param cacheName The cache containing the list.
   * @param listName The list in which to add the value.
   * @param value The value to add to the list.
   * @param truncateFrontToSize If the list exceeds this length, remove excess from the front of the
   *     list. Must be positive. Will not truncate if not provided.
   * @param ttl TTL for the set in cache. This TTL takes precedence over the TTL used when
   *     initializing a cache client. Defaults to the client's TTL if not provided.
   * @return Future containing the result of the list push back operation: {@link
   *     CacheListPushBackResponse.Success} or {@link CacheListPushBackResponse.Error}.
   */
  public CompletableFuture<CacheListPushBackResponse> listPushBack(
      @Nonnull String cacheName,
      @Nonnull String listName,
      @Nonnull String value,
      @Nullable Integer truncateFrontToSize,
      @Nullable CollectionTtl ttl) {
    return scsDataClient.listPushBack(cacheName, listName, value, truncateFrontToSize, ttl);
  }

  /**
   * Pushes a value to the back of a list. Refreshes the list with the client's default TTL.
   *
   * @param cacheName The cache containing the list.
   * @param listName The list in which to add the value.
   * @param value The value to add to the list.
   * @param truncateFrontToSize If the list exceeds this length, remove excess from the front of the
   *     list. Must be positive. Will not truncate if not provided.
   * @return Future containing the result of the list push back operation: {@link
   *     CacheListPushBackResponse.Success} or {@link CacheListPushBackResponse.Error}.
   */
  public CompletableFuture<CacheListPushBackResponse> listPushBack(
      @Nonnull String cacheName,
      @Nonnull String listName,
      @Nonnull String value,
      @Nullable Integer truncateFrontToSize) {
    return scsDataClient.listPushBack(cacheName, listName, value, truncateFrontToSize, null);
  }

  /**
   * Pushes a value to the back of a list. Refreshes the list with the client's default TTL.
   *
   * @param cacheName The cache containing the list.
   * @param listName The list in which to add the value.
   * @param value The value to add to the list.
   * @return Future containing the result of the list push back operation: {@link
   *     CacheListPushBackResponse.Success} or {@link CacheListPushBackResponse.Error}.
   */
  public CompletableFuture<CacheListPushBackResponse> listPushBack(
      @Nonnull String cacheName, @Nonnull String listName, @Nonnull String value) {
    return scsDataClient.listPushBack(cacheName, listName, value, null, null);
  }

  /**
   * Pushes a value to the back of a list.
   *
   * @param cacheName The cache containing the list.
   * @param listName The list in which to add the value.
   * @param value The value to add to the list.
   * @param truncateFrontToSize If the list exceeds this length, remove excess from the front of the
   *     list. Must be positive. Will not truncate if not provided.
   * @param ttl TTL for the set in cache. This TTL takes precedence over the TTL used when
   *     initializing a cache client. Defaults to the client's TTL if not provided.
   * @return Future containing the result of the list push back operation: {@link
   *     CacheListPushBackResponse.Success} or {@link CacheListPushBackResponse.Error}.
   */
  public CompletableFuture<CacheListPushBackResponse> listPushBack(
      @Nonnull String cacheName,
      @Nonnull String listName,
      @Nonnull byte[] value,
      @Nullable Integer truncateFrontToSize,
      @Nullable CollectionTtl ttl) {
    return scsDataClient.listPushBack(cacheName, listName, value, truncateFrontToSize, ttl);
  }

  /**
   * Pushes a value to the back of a list. Refreshes the list with the client's default TTL.
   *
   * @param cacheName The cache containing the list.
   * @param listName The list in which to add the value.
   * @param value The value to add to the list.
   * @param truncateFrontToSize If the list exceeds this length, remove excess from the front of the
   *     list. Must be positive. Will not truncate if not provided.
   * @return Future containing the result of the list push back operation: {@link
   *     CacheListPushBackResponse.Success} or {@link CacheListPushBackResponse.Error}.
   */
  public CompletableFuture<CacheListPushBackResponse> listPushBack(
      @Nonnull String cacheName,
      @Nonnull String listName,
      @Nonnull byte[] value,
      @Nullable Integer truncateFrontToSize) {
    return scsDataClient.listPushBack(cacheName, listName, value, truncateFrontToSize, null);
  }

  /**
   * Pushes a value to the back of a list. Refreshes the list with the client's default TTL.
   *
   * @param cacheName The cache containing the list.
   * @param listName The list in which to add the value.
   * @param value The value to add to the list.
   * @return Future containing the result of the list push back operation: {@link
   *     CacheListPushBackResponse.Success} or {@link CacheListPushBackResponse.Error}.
   */
  public CompletableFuture<CacheListPushBackResponse> listPushBack(
      @Nonnull String cacheName, @Nonnull String listName, @Nonnull byte[] value) {
    return scsDataClient.listPushBack(cacheName, listName, value, null, null);
  }

  /**
   * Pushes a value to the front of a list.
   *
   * @param cacheName The cache containing the list.
   * @param listName The list in which to add the value.
   * @param value The value to add to the list.
   * @param truncateBackToSize If the list exceeds this length, remove excess from the front of the
   *     list. Must be positive. Will not truncate if not provided.
   * @param ttl TTL for the set in cache. This TTL takes precedence over the TTL used when
   *     initializing a cache client. Defaults to the client's TTL if not provided.
   * @return Future containing the result of the list push front operation: {@link
   *     CacheListPushFrontResponse.Success} or {@link CacheListPushFrontResponse.Error}.
   */
  public CompletableFuture<CacheListPushFrontResponse> listPushFront(
      @Nonnull String cacheName,
      @Nonnull String listName,
      @Nonnull String value,
      @Nullable Integer truncateBackToSize,
      @Nullable CollectionTtl ttl) {
    return scsDataClient.listPushFront(cacheName, listName, value, truncateBackToSize, ttl);
  }

  /**
   * Pushes a value to the front of a list. Refreshes the list with the client's default TTL.
   *
   * @param cacheName The cache containing the list.
   * @param listName The list in which to add the value.
   * @param value The value to add to the list.
   * @param truncateBackToSize If the list exceeds this length, remove excess from the front of the
   *     list. Must be positive. Will not truncate if not provided.
   * @return Future containing the result of the list push front operation: {@link
   *     CacheListPushFrontResponse.Success} or {@link CacheListPushFrontResponse.Error}.
   */
  public CompletableFuture<CacheListPushFrontResponse> listPushFront(
      @Nonnull String cacheName,
      @Nonnull String listName,
      @Nonnull String value,
      @Nullable Integer truncateBackToSize) {
    return scsDataClient.listPushFront(cacheName, listName, value, truncateBackToSize, null);
  }

  /**
   * Pushes a value to the front of a list. Refreshes the list with the client's default TTL.
   *
   * @param cacheName The cache containing the list.
   * @param listName The list in which to add the value.
   * @param value The value to add to the list.
   * @return Future containing the result of the list push front operation: {@link
   *     CacheListPushFrontResponse.Success} or {@link CacheListPushFrontResponse.Error}.
   */
  public CompletableFuture<CacheListPushFrontResponse> listPushFront(
      @Nonnull String cacheName, @Nonnull String listName, @Nonnull String value) {
    return scsDataClient.listPushFront(cacheName, listName, value, null, null);
  }

  /**
   * Pushes a value to the front of a list.
   *
   * @param cacheName The cache containing the list.
   * @param listName The list in which to add the value.
   * @param value The value to add to the list.
   * @param truncateBackToSize If the list exceeds this length, remove excess from the front of the
   *     list. Must be positive. Will not truncate if not provided.
   * @param ttl TTL for the set in cache. This TTL takes precedence over the TTL used when
   *     initializing a cache client. Defaults to the client's TTL if not provided.
   * @return Future containing the result of the list push front operation: {@link
   *     CacheListPushFrontResponse.Success} or {@link CacheListPushFrontResponse.Error}.
   */
  public CompletableFuture<CacheListPushFrontResponse> listPushFront(
      @Nonnull String cacheName,
      @Nonnull String listName,
      @Nonnull byte[] value,
      @Nullable Integer truncateBackToSize,
      @Nullable CollectionTtl ttl) {
    return scsDataClient.listPushFront(cacheName, listName, value, truncateBackToSize, ttl);
  }

  /**
   * Pushes a value to the front of a list. Refreshes the list with the client's default TTL.
   *
   * @param cacheName The cache containing the list.
   * @param listName The list in which to add the value.
   * @param value The value to add to the list.
   * @param truncateBackToSize If the list exceeds this length, remove excess from the front of the
   *     list. Must be positive. Will not truncate if not provided.
   * @return Future containing the result of the list push front operation: {@link
   *     CacheListPushFrontResponse.Success} or {@link CacheListPushFrontResponse.Error}.
   */
  public CompletableFuture<CacheListPushFrontResponse> listPushFront(
      @Nonnull String cacheName,
      @Nonnull String listName,
      @Nonnull byte[] value,
      @Nullable Integer truncateBackToSize) {
    return scsDataClient.listPushFront(cacheName, listName, value, truncateBackToSize, null);
  }

  /**
   * Pushes a value to the front of a list. Refreshes the list with the client's default TTL.
   *
   * @param cacheName The cache containing the list.
   * @param listName The list in which to add the value.
   * @param value The value to add to the list.
   * @return Future containing the result of the list push front operation: {@link
   *     CacheListPushFrontResponse.Success} or {@link CacheListPushFrontResponse.Error}.
   */
  public CompletableFuture<CacheListPushFrontResponse> listPushFront(
      @Nonnull String cacheName, @Nonnull String listName, @Nonnull byte[] value) {
    return scsDataClient.listPushFront(cacheName, listName, value, null, null);
  }

  /**
   * Removes a value from a list.
   *
   * @param cacheName The cache containing the list.
   * @param listName The list to remove the value from.
   * @param value The value to remove.
   * @return Future containing the result of the list remove value operation: {@link
   *     CacheListRemoveValueResponse.Success} or {@link CacheListRemoveValueResponse.Error}.
   */
  public CompletableFuture<CacheListRemoveValueResponse> listRemoveValue(
      @Nonnull String cacheName, @Nonnull String listName, @Nonnull String value) {
    return scsDataClient.listRemoveValue(cacheName, listName, value);
  }

  /**
   * Removes a value from a list.
   *
   * @param cacheName The cache containing the list.
   * @param listName The list to remove the value from.
   * @param value The value to remove.
   * @return Future containing the result of the list remove value operation: {@link
   *     CacheListRemoveValueResponse.Success} or {@link CacheListRemoveValueResponse.Error}.
   */
  public CompletableFuture<CacheListRemoveValueResponse> listRemoveValue(
      @Nonnull String cacheName, @Nonnull String listName, @Nonnull byte[] value) {
    return scsDataClient.listRemoveValue(cacheName, listName, value);
  }

  /**
   * Retains only the elements of a list that are between the given indices.
   *
   * @param cacheName The cache containing the list.
   * @param listName The list to cut down.
   * @param startIndex - Start index (inclusive) for list retain operation.
   * @param endIndex - End index (exclusive) for list retain operation.
   * @return Future containing the result of the list retain value operation: {@link
   *     CacheListRetainResponse.Success} or {@link CacheListRetainResponse.Error}.
   */
  public CompletableFuture<CacheListRetainResponse> listRetain(
      @Nonnull String cacheName,
      @Nonnull String listName,
      @Nullable Integer startIndex,
      @Nullable Integer endIndex) {
    return scsDataClient.listRetain(cacheName, listName, startIndex, endIndex);
  }

  /**
   * Fetches all elements of a dictionary.
   *
   * @param cacheName The cache containing the dictionary.
   * @param dictionaryName The dictionary to fetch.
   * @return Future containing the result of the dictionary fetch operation: {@link
   *     CacheDictionaryFetchResponse.Hit} containing the dictionary, {@link
   *     CacheDictionaryFetchResponse.Miss} if no dictionary was found, or {@link
   *     CacheDictionaryFetchResponse.Error}.
   */
  public CompletableFuture<CacheDictionaryFetchResponse> dictionaryFetch(
      @Nonnull String cacheName, @Nonnull String dictionaryName) {
    return scsDataClient.dictionaryFetch(cacheName, dictionaryName);
  }

  /**
   * Sets a field to the given value in a dictionary.
   *
   * @param cacheName The cache containing the dictionary.
   * @param dictionaryName The dictionary to set the field in.
   * @param field The field to set.
   * @param value The value to set.
   * @param ttl TTL for the set in cache. This TTL takes precedence over the TTL used when
   *     initializing a cache client. Defaults to the client's TTL if not provided.
   * @return Future containing the result of the dictionary set field operation: {@link
   *     CacheDictionarySetFieldResponse.Success} or {@link CacheDictionarySetFieldResponse.Error}.
   */
  public CompletableFuture<CacheDictionarySetFieldResponse> dictionarySetField(
      @Nonnull String cacheName,
      @Nonnull String dictionaryName,
      @Nonnull String field,
      @Nonnull String value,
      @Nullable CollectionTtl ttl) {
    return scsDataClient.dictionarySetField(cacheName, dictionaryName, field, value, ttl);
  }

  /**
   * Sets a field in the given dictionary.
   *
   * @param cacheName The cache containing the dictionary.
   * @param dictionaryName The dictionary to set the field in.
   * @param field The field to set.
   * @param value The value to set.
   * @return Future containing the result of the dictionary set field operation: {@link
   *     CacheDictionarySetFieldResponse.Success} or {@link CacheDictionarySetFieldResponse.Error}.
   */
  public CompletableFuture<CacheDictionarySetFieldResponse> dictionarySetField(
      @Nonnull String cacheName,
      @Nonnull String dictionaryName,
      @Nonnull String field,
      @Nonnull String value) {
    return scsDataClient.dictionarySetField(cacheName, dictionaryName, field, value, null);
  }

  /**
   * Sets a field in the given dictionary.
   *
   * @param cacheName The cache containing the dictionary.
   * @param dictionaryName The dictionary to set the field in.
   * @param field The field to set.
   * @param value The value to set.
   * @param ttl TTL for the set in cache. This TTL takes precedence over the TTL used when
   *     initializing a cache client. Defaults to the client's TTL if not provided.
   * @return Future containing the result of the dictionary set field operation: {@link
   *     CacheDictionarySetFieldResponse.Success} or {@link CacheDictionarySetFieldResponse.Error}.
   */
  public CompletableFuture<CacheDictionarySetFieldResponse> dictionarySetField(
      @Nonnull String cacheName,
      @Nonnull String dictionaryName,
      @Nonnull String field,
      @Nonnull byte[] value,
      @Nullable CollectionTtl ttl) {
    return scsDataClient.dictionarySetField(cacheName, dictionaryName, field, value, ttl);
  }

  /**
   * Sets a field in the given dictionary.
   *
   * @param cacheName The cache containing the dictionary.
   * @param dictionaryName The dictionary to set the field in.
   * @param field The field to set.
   * @param value The value to set.
   * @return Future containing the result of the dictionary set field operation: {@link
   *     CacheDictionarySetFieldResponse.Success} or {@link CacheDictionarySetFieldResponse.Error}.
   */
  public CompletableFuture<CacheDictionarySetFieldResponse> dictionarySetField(
      @Nonnull String cacheName,
      @Nonnull String dictionaryName,
      @Nonnull String field,
      @Nonnull byte[] value) {
    return scsDataClient.dictionarySetField(cacheName, dictionaryName, field, value, null);
  }

  /**
   * Sets all the given fields in a dictionary.
   *
   * @param cacheName The cache containing the dictionary.
   * @param dictionaryName The dictionary to set the field in.
   * @param elements The fields to set.
   * @param ttl TTL for the set in cache. This TTL takes precedence over the TTL used when
   *     initializing a cache client. Defaults to the client's TTL if not provided.
   * @return Future containing the result of the dictionary set field operation: {@link
   *     CacheDictionarySetFieldsResponse.Success} or {@link
   *     CacheDictionarySetFieldsResponse.Error}.
   */
  public CompletableFuture<CacheDictionarySetFieldsResponse> dictionarySetFields(
      @Nonnull String cacheName,
      @Nonnull String dictionaryName,
      @Nonnull Map<String, String> elements,
      @Nullable CollectionTtl ttl) {
    return scsDataClient.dictionarySetFields(cacheName, dictionaryName, elements, ttl);
  }

  /**
   * Sets all the given fields in a dictionary. Refreshes the dictionary with the client's default
   * TTL.
   *
   * @param cacheName The cache containing the dictionary.
   * @param dictionaryName The dictionary to set the fields in.
   * @param elements The fields to set.
   * @return Future containing the result of the dictionary set field operation: {@link
   *     CacheDictionarySetFieldsResponse.Success} or {@link
   *     CacheDictionarySetFieldsResponse.Error}.
   */
  public CompletableFuture<CacheDictionarySetFieldsResponse> dictionarySetFields(
      @Nonnull String cacheName,
      @Nonnull String dictionaryName,
      @Nonnull Map<String, String> elements) {
    return scsDataClient.dictionarySetFields(cacheName, dictionaryName, elements, null);
  }

  /**
   * Sets all the given fields in a dictionary.
   *
   * @param cacheName The cache containing the dictionary.
   * @param dictionaryName The dictionary to set the fields in.
   * @param elements The fields to set.
   * @param ttl TTL for the set in cache. This TTL takes precedence over the TTL used when
   *     initializing a cache client. Defaults to the client's TTL if not provided.
   * @return Future containing the result of the dictionary set field operation: {@link
   *     CacheDictionarySetFieldsResponse.Success} or {@link
   *     CacheDictionarySetFieldsResponse.Error}.
   */
  public CompletableFuture<CacheDictionarySetFieldsResponse> dictionarySetFieldsStringBytes(
      @Nonnull String cacheName,
      @Nonnull String dictionaryName,
      @Nonnull Map<String, byte[]> elements,
      @Nullable CollectionTtl ttl) {
    return scsDataClient.dictionarySetFieldsStringBytes(cacheName, dictionaryName, elements, ttl);
  }

  /**
   * Sets all the given fields in a dictionary. Refreshes the dictionary with the client's default
   * TTL.
   *
   * @param cacheName The cache containing the dictionary.
   * @param dictionaryName The dictionary to set the fields in.
   * @param elements The fields to set.
   * @return Future containing the result of the dictionary set field operation: {@link
   *     CacheDictionarySetFieldsResponse.Success} or {@link
   *     CacheDictionarySetFieldsResponse.Error}.
   */
  public CompletableFuture<CacheDictionarySetFieldsResponse> dictionarySetFieldsStringBytes(
      @Nonnull String cacheName,
      @Nonnull String dictionaryName,
      @Nonnull Map<String, byte[]> elements) {
    return scsDataClient.dictionarySetFieldsStringBytes(cacheName, dictionaryName, elements, null);
  }

  /**
   * Gets the value for the given field from a dictionary.
   *
   * @param cacheName The cache containing the dictionary.
   * @param dictionaryName The dictionary to get the value from.
   * @param field The field to look up.
   * @return Future containing the result of the dictionary get field operation: {@link
   *     CacheDictionaryGetFieldResponse.Hit} containing the field and value, {@link
   *     CacheDictionaryGetFieldResponse.Miss} if the field was not found in the dictionary, or
   *     {@link CacheDictionaryGetFieldResponse.Error}.
   */
  public CompletableFuture<CacheDictionaryGetFieldResponse> dictionaryGetField(
      @Nonnull String cacheName, @Nonnull String dictionaryName, @Nonnull String field) {
    return scsDataClient.dictionaryGetField(cacheName, dictionaryName, field);
  }

  /**
   * Gets the values for the given fields from a dictionary.
   *
   * @param cacheName The cache containing the dictionary.
   * @param dictionaryName The dictionary to get the values from.
   * @param fields The fields to look up.
   * @return Future containing the result of the dictionary get fields operation: {@link
   *     CacheDictionaryGetFieldsResponse.Hit} containing the fields, values, and a per-field {@link
   *     CacheDictionaryGetFieldResponse}, {@link CacheDictionaryGetFieldsResponse.Miss} if none of
   *     the fields were found in the dictionary, or {@link CacheDictionaryGetFieldsResponse.Error}.
   */
  public CompletableFuture<CacheDictionaryGetFieldsResponse> dictionaryGetFields(
      @Nonnull String cacheName, @Nonnull String dictionaryName, @Nonnull Iterable<String> fields) {
    return scsDataClient.dictionaryGetFields(cacheName, dictionaryName, fields);
  }

  /**
   * Increments the given field's value in a dictionary.
   *
   * @param cacheName The cache containing the dictionary.
   * @param dictionaryName The dictionary to increment the value in.
   * @param field The field for which the value is to be incremented.
   * @param amount The amount to increment.
   * @param ttl TTL for the set in cache. This TTL takes precedence over the TTL used when
   *     initializing a cache client. Defaults to the client's TTL if not provided.
   * @return Future containing the result of the dictionary increment operation: {@link
   *     CacheDictionaryIncrementResponse.Success} or {@link
   *     CacheDictionaryIncrementResponse.Error}.
   */
  public CompletableFuture<CacheDictionaryIncrementResponse> dictionaryIncrement(
      @Nonnull String cacheName,
      @Nonnull String dictionaryName,
      @Nonnull String field,
      long amount,
      @Nullable CollectionTtl ttl) {
    return scsDataClient.dictionaryIncrement(cacheName, dictionaryName, field, amount, ttl);
  }

  /**
   * Increments the given field's value in a dictionary. Refreshes the dictionary with the client's
   * default TTL.
   *
   * @param cacheName The cache containing the dictionary.
   * @param dictionaryName The dictionary to increment the value in.
   * @param field The field for which the value is to be incremented.
   * @param amount The amount to increment.
   * @return Future containing the result of the dictionary increment operation: {@link
   *     CacheDictionaryIncrementResponse.Success} or {@link
   *     CacheDictionaryIncrementResponse.Error}.
   */
  public CompletableFuture<CacheDictionaryIncrementResponse> dictionaryIncrement(
      @Nonnull String cacheName,
      @Nonnull String dictionaryName,
      @Nonnull String field,
      long amount) {
    return scsDataClient.dictionaryIncrement(cacheName, dictionaryName, field, amount, null);
  }

  /**
   * Removes the given field from a dictionary.
   *
   * @param cacheName The cache containing the dictionary.
   * @param dictionaryName The dictionary to remove the field from.
   * @param field The field to remove.
   * @return Future containing the result of the dictionary remove field operation: {@link
   *     CacheDictionaryRemoveFieldResponse.Success} or {@link
   *     CacheDictionaryRemoveFieldResponse.Error}.
   */
  public CompletableFuture<CacheDictionaryRemoveFieldResponse> dictionaryRemoveField(
      @Nonnull String cacheName, @Nonnull String dictionaryName, @Nonnull String field) {
    return scsDataClient.dictionaryRemoveField(cacheName, dictionaryName, field);
  }

  /**
   * Removes the given fields from a dictionary.
   *
   * @param cacheName The cache containing the dictionary.
   * @param dictionaryName The dictionary to remove the fields from.
   * @param fields The fields to remove.
   * @return Future containing the result of the dictionary remove field operation: {@link
   *     CacheDictionaryRemoveFieldsResponse.Success} or {@link
   *     CacheDictionaryRemoveFieldsResponse.Error}.
   */
  public CompletableFuture<CacheDictionaryRemoveFieldsResponse> dictionaryRemoveFields(
      @Nonnull String cacheName, @Nonnull String dictionaryName, @Nonnull Iterable<String> fields) {
    return scsDataClient.dictionaryRemoveFields(cacheName, dictionaryName, fields);
  }

  @Override
  public void close() {
    scsControlClient.close();
    scsDataClient.close();
  }
}
