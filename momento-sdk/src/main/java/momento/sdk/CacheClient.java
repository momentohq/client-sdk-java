package momento.sdk;

import java.io.Closeable;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import momento.sdk.auth.CredentialProvider;
import momento.sdk.config.Configuration;
import momento.sdk.exceptions.InvalidArgumentException;
import momento.sdk.requests.CollectionTtl;
import momento.sdk.responses.SortOrder;
import momento.sdk.responses.cache.DeleteResponse;
import momento.sdk.responses.cache.GetResponse;
import momento.sdk.responses.cache.IncrementResponse;
import momento.sdk.responses.cache.SetIfNotExistsResponse;
import momento.sdk.responses.cache.SetResponse;
import momento.sdk.responses.cache.control.CacheCreateResponse;
import momento.sdk.responses.cache.control.CacheDeleteResponse;
import momento.sdk.responses.cache.control.CacheFlushResponse;
import momento.sdk.responses.cache.control.CacheListResponse;
import momento.sdk.responses.cache.dictionary.DictionaryFetchResponse;
import momento.sdk.responses.cache.dictionary.DictionaryGetFieldResponse;
import momento.sdk.responses.cache.dictionary.DictionaryGetFieldsResponse;
import momento.sdk.responses.cache.dictionary.DictionaryIncrementResponse;
import momento.sdk.responses.cache.dictionary.DictionaryRemoveFieldResponse;
import momento.sdk.responses.cache.dictionary.DictionaryRemoveFieldsResponse;
import momento.sdk.responses.cache.dictionary.DictionarySetFieldResponse;
import momento.sdk.responses.cache.dictionary.DictionarySetFieldsResponse;
import momento.sdk.responses.cache.list.ListConcatenateBackResponse;
import momento.sdk.responses.cache.list.ListConcatenateFrontResponse;
import momento.sdk.responses.cache.list.ListFetchResponse;
import momento.sdk.responses.cache.list.ListLengthResponse;
import momento.sdk.responses.cache.list.ListPopBackResponse;
import momento.sdk.responses.cache.list.ListPopFrontResponse;
import momento.sdk.responses.cache.list.ListPushBackResponse;
import momento.sdk.responses.cache.list.ListPushFrontResponse;
import momento.sdk.responses.cache.list.ListRemoveValueResponse;
import momento.sdk.responses.cache.list.ListRetainResponse;
import momento.sdk.responses.cache.set.SetAddElementResponse;
import momento.sdk.responses.cache.set.SetAddElementsResponse;
import momento.sdk.responses.cache.set.SetFetchResponse;
import momento.sdk.responses.cache.set.SetRemoveElementResponse;
import momento.sdk.responses.cache.set.SetRemoveElementsResponse;
import momento.sdk.responses.cache.signing.SigningKeyCreateResponse;
import momento.sdk.responses.cache.signing.SigningKeyListResponse;
import momento.sdk.responses.cache.signing.SigningKeyRevokeResponse;
import momento.sdk.responses.cache.sortedset.ScoredElement;
import momento.sdk.responses.cache.sortedset.SortedSetFetchResponse;
import momento.sdk.responses.cache.sortedset.SortedSetGetRankResponse;
import momento.sdk.responses.cache.sortedset.SortedSetGetScoreResponse;
import momento.sdk.responses.cache.sortedset.SortedSetGetScoresResponse;
import momento.sdk.responses.cache.sortedset.SortedSetIncrementScoreResponse;
import momento.sdk.responses.cache.sortedset.SortedSetPutElementResponse;
import momento.sdk.responses.cache.sortedset.SortedSetPutElementsResponse;
import momento.sdk.responses.cache.sortedset.SortedSetRemoveElementResponse;
import momento.sdk.responses.cache.sortedset.SortedSetRemoveElementsResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Client to perform operations against the Momento Cache Service */
public final class CacheClient implements Closeable {

  private final Logger logger = LoggerFactory.getLogger(CacheClient.class);

  private final ScsControlClient scsControlClient;
  private final ScsDataClient scsDataClient;

  private static final long DEFAULT_EAGER_CONNECTION_TIMEOUT_SECONDS = 30;

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

    logger.info("Creating Momento Cache Client");
    logger.debug("Cache endpoint: " + credentialProvider.getCacheEndpoint());
    logger.debug("Control endpoint: " + credentialProvider.getControlEndpoint());
  }

  /**
   * Constructs a CacheClient.
   *
   * @param credentialProvider Provider for the credentials required to connect to Momento.
   * @param configuration Configuration object containing all tunable client settings.
   * @param itemDefaultTtl The default TTL for values written to a cache.
   * @return CacheClient
   */
  public static CacheClient create(
          @Nonnull CredentialProvider credentialProvider,
          @Nonnull Configuration configuration,
          @Nonnull Duration itemDefaultTtl) {
    return create(credentialProvider, configuration, itemDefaultTtl, null /* eagerConnectionTimeout */);
  }

  /**
   * Constructs a CacheClient.
   *
   * @param credentialProvider Provider for the credentials required to connect to Momento.
   * @param configuration Configuration object containing all tunable client settings.
   * @param itemDefaultTtl The default TTL for values written to a cache.
   * @param eagerConnectionTimeout The timeout value while trying to establish an eager connection
   *     with the Momento server.
   * @return CacheClient
   */
  public static CacheClient create(
      @Nonnull CredentialProvider credentialProvider,
      @Nonnull Configuration configuration,
      @Nonnull Duration itemDefaultTtl,
      @Nullable Duration eagerConnectionTimeout) {
    final CacheClient client =
        CacheClient.builder(credentialProvider, configuration, itemDefaultTtl).build();

    final long timeout;
    if (eagerConnectionTimeout == null) {
      timeout = DEFAULT_EAGER_CONNECTION_TIMEOUT_SECONDS;
    } else {
      timeout = eagerConnectionTimeout.getSeconds();
    }

    checkTimeoutValid(timeout);

    // a client can explicitly set the timeout to be 0 in which case we don't want to establish an
    // eager connection
    if (timeout != 0) {
      client.scsDataClient.connect(timeout);
    }

    return client;
  }

  private static void checkTimeoutValid(long timeout) {
    if (timeout < 0) {
      throw new InvalidArgumentException("Timeout must be positive");
    }
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
   *     CacheCreateResponse.Success} or {@link CacheCreateResponse.Error}.
   */
  public CompletableFuture<CacheCreateResponse> createCache(String cacheName) {
    return scsControlClient.createCache(cacheName);
  }

  /**
   * Deletes a cache.
   *
   * @param cacheName The cache to be deleted.
   * @return A future containing the result of the cache deletion: {@link
   *     CacheDeleteResponse.Success} or {@link CacheDeleteResponse.Error}.
   */
  public CompletableFuture<CacheDeleteResponse> deleteCache(String cacheName) {
    return scsControlClient.deleteCache(cacheName);
  }

  /**
   * Flushes the contents of a cache.
   *
   * @param cacheName The cache to be flushed.
   * @return A future containing the result of the cache flush: {@link CacheFlushResponse.Success}
   *     or {@link CacheFlushResponse.Error}.
   */
  public CompletableFuture<CacheFlushResponse> flushCache(String cacheName) {
    return scsControlClient.flushCache(cacheName);
  }

  /**
   * Lists all caches.
   *
   * @return A future containing the result of the list caches operation: {@link
   *     CacheListResponse.Success} containing the list of caches, or {@link
   *     CacheListResponse.Error}.
   */
  public CompletableFuture<CacheListResponse> listCaches() {
    return scsControlClient.listCaches();
  }

  /**
   * Creates a new Momento signing key.
   *
   * @param ttl The key's time-to-live duration.
   * @return A future containing the result of the signing key creation: {@link
   *     SigningKeyCreateResponse.Success} containing the key and its metadata, or {@link
   *     SigningKeyCreateResponse.Error}.
   */
  public CompletableFuture<SigningKeyCreateResponse> createSigningKey(Duration ttl) {
    return scsControlClient.createSigningKey(ttl);
  }

  /**
   * Revokes a Momento signing key and invalidates all tokens signed by it.
   *
   * @param keyId The ID of the key to revoke.
   * @return A future containing the result of the signing key revocation: {@link
   *     SigningKeyRevokeResponse.Success} or {@link SigningKeyRevokeResponse.Error}.
   */
  public CompletableFuture<SigningKeyRevokeResponse> revokeSigningKey(String keyId) {
    return scsControlClient.revokeSigningKey(keyId);
  }

  /**
   * Lists all Momento signing keys.
   *
   * @return A future containing the result of the signing key revocation: {@link
   *     SigningKeyListResponse.Success} containing the list of signing keys, or {@link
   *     SigningKeyListResponse.Error}.
   */
  public CompletableFuture<SigningKeyListResponse> listSigningKeys() {
    return scsControlClient.listSigningKeys();
  }

  /**
   * Get the cache value stored for the given key.
   *
   * @param cacheName Name of the cache to get the item from.
   * @param key The key to get.
   * @return Future with {@link GetResponse} containing the status of the get operation and the
   *     associated value data.
   */
  public CompletableFuture<GetResponse> get(String cacheName, byte[] key) {
    return scsDataClient.get(cacheName, key);
  }

  /**
   * Get the cache value stored for the given key.
   *
   * @param cacheName Name of the cache to get the item from.
   * @param key The key to get.
   * @return Future with {@link GetResponse} containing the status of the get operation and the
   *     associated value data.
   */
  public CompletableFuture<GetResponse> get(String cacheName, String key) {
    return scsDataClient.get(cacheName, key);
  }

  /**
   * Delete the value stored in Momento cache.
   *
   * @param cacheName Name of the cache to delete the item from.
   * @param key The key to delete
   * @return Future with {@link DeleteResponse}.
   */
  public CompletableFuture<DeleteResponse> delete(String cacheName, String key) {
    return scsDataClient.delete(cacheName, key);
  }

  /**
   * Delete the value stored in Momento cache.
   *
   * @param cacheName Name of the cache to delete the item from.
   * @param key The key to delete.
   * @return Future with {@link DeleteResponse}.
   */
  public CompletableFuture<DeleteResponse> delete(String cacheName, byte[] key) {
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
  public CompletableFuture<SetResponse> set(
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
  public CompletableFuture<SetResponse> set(String cacheName, byte[] key, byte[] value) {
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
  public CompletableFuture<SetResponse> set(
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
  public CompletableFuture<SetResponse> set(String cacheName, String key, String value) {
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
  public CompletableFuture<SetIfNotExistsResponse> setIfNotExists(
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
  public CompletableFuture<SetIfNotExistsResponse> setIfNotExists(
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
  public CompletableFuture<SetIfNotExistsResponse> setIfNotExists(
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
  public CompletableFuture<SetIfNotExistsResponse> setIfNotExists(
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
  public CompletableFuture<SetIfNotExistsResponse> setIfNotExists(
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
  public CompletableFuture<SetIfNotExistsResponse> setIfNotExists(
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
  public CompletableFuture<SetIfNotExistsResponse> setIfNotExists(
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
  public CompletableFuture<SetIfNotExistsResponse> setIfNotExists(
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
  public CompletableFuture<IncrementResponse> increment(
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
  public CompletableFuture<IncrementResponse> increment(
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
  public CompletableFuture<IncrementResponse> increment(
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
  public CompletableFuture<IncrementResponse> increment(
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
   * @return Future containing the result of the add element operation: {@link
   *     SetAddElementResponse.Success} or {@link SetAddElementResponse.Error}.
   */
  public CompletableFuture<SetAddElementResponse> setAddElement(
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
   * @return Future containing the result of the add element operation: {@link
   *     SetAddElementResponse.Success} or {@link SetAddElementResponse.Error}.
   */
  public CompletableFuture<SetAddElementResponse> setAddElement(
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
   * @return Future containing the result of the add element operation: {@link
   *     SetAddElementResponse.Success} or {@link SetAddElementResponse.Error}.
   */
  public CompletableFuture<SetAddElementResponse> setAddElement(
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
   * @return Future containing the result of the add element operation: {@link
   *     SetAddElementResponse.Success} or {@link SetAddElementResponse.Error}.
   */
  public CompletableFuture<SetAddElementResponse> setAddElement(
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
   * @return Future containing the result of the add elements operation: {@link
   *     SetAddElementsResponse.Success} or {@link SetAddElementsResponse.Error}.
   */
  public CompletableFuture<SetAddElementsResponse> setAddElements(
      @Nonnull String cacheName,
      @Nonnull String setName,
      @Nonnull Iterable<String> elements,
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
   * @return Future containing the result of the add elements operation: {@link
   *     SetAddElementsResponse.Success} or {@link SetAddElementsResponse.Error}.
   */
  public CompletableFuture<SetAddElementsResponse> setAddElements(
      @Nonnull String cacheName, @Nonnull String setName, @Nonnull Iterable<String> elements) {
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
   * @return Future containing the result of the add elements operation: {@link
   *     SetAddElementsResponse.Success} or {@link SetAddElementsResponse.Error}.
   */
  public CompletableFuture<SetAddElementsResponse> setAddElementsByteArray(
      @Nonnull String cacheName,
      @Nonnull String setName,
      @Nonnull Iterable<byte[]> elements,
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
   * @return Future containing the result of the add elements operation: {@link
   *     SetAddElementsResponse.Success} or {@link SetAddElementsResponse.Error}.
   */
  public CompletableFuture<SetAddElementsResponse> setAddElementsByteArray(
      @Nonnull String cacheName, @Nonnull String setName, @Nonnull Iterable<byte[]> elements) {
    return scsDataClient.setAddElementsByteArray(cacheName, setName, elements, null);
  }

  /**
   * Remove an element from a set.
   *
   * @param cacheName Name of the cache containing the set.
   * @param setName The set to remove the element from.
   * @param element The value to remove from the set.
   * @return Future containing the result of the remove element operation: {@link
   *     SetRemoveElementResponse.Success} or {@link SetRemoveElementResponse.Error}.
   */
  public CompletableFuture<SetRemoveElementResponse> setRemoveElement(
      @Nonnull String cacheName, @Nonnull String setName, @Nonnull String element) {
    return scsDataClient.setRemoveElement(cacheName, setName, element);
  }

  /**
   * Remove an element from a set.
   *
   * @param cacheName Name of the cache containing the set.
   * @param setName The set to remove the element from.
   * @param element The value to remove from the set.
   * @return Future containing the result of the remove element operation: {@link
   *     SetRemoveElementResponse.Success} or {@link SetRemoveElementResponse.Error}.
   */
  public CompletableFuture<SetRemoveElementResponse> setRemoveElement(
      @Nonnull String cacheName, @Nonnull String setName, @Nonnull byte[] element) {
    return scsDataClient.setRemoveElement(cacheName, setName, element);
  }

  /**
   * Remove several elements from a set.
   *
   * @param cacheName Name of the cache containing the set.
   * @param setName The set to remove the elements from.
   * @param elements The values to remove from the set.
   * @return Future containing the result of the remove elements operation: {@link
   *     SetRemoveElementsResponse.Success} or {@link SetRemoveElementsResponse.Error}.
   */
  public CompletableFuture<SetRemoveElementsResponse> setRemoveElements(
      @Nonnull String cacheName, @Nonnull String setName, @Nonnull Iterable<String> elements) {
    return scsDataClient.setRemoveElements(cacheName, setName, elements);
  }

  /**
   * Remove several elements from a set.
   *
   * @param cacheName Name of the cache containing the set.
   * @param setName The set to remove the elements from.
   * @param elements The value to remove from the set.
   * @return Future containing the result of the remove elements operation: {@link
   *     SetRemoveElementsResponse.Success} or {@link SetRemoveElementsResponse.Error}.
   */
  public CompletableFuture<SetRemoveElementsResponse> setRemoveElementsByteArray(
      @Nonnull String cacheName, @Nonnull String setName, @Nonnull Iterable<byte[]> elements) {
    return scsDataClient.setRemoveElementsByteArray(cacheName, setName, elements);
  }

  /**
   * Fetch an entire set from the cache.
   *
   * @param cacheName Name of the cache to perform the lookup in.
   * @param setName The set to fetch.
   * @return Future containing the result of the fetch operation: {@link SetFetchResponse.Hit},
   *     {@link SetFetchResponse.Miss}, or {@link SetFetchResponse.Error}.
   */
  public CompletableFuture<SetFetchResponse> setFetch(
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
   * @return Future containing the result of the put element operation: {@link
   *     SortedSetPutElementResponse.Success} or {@link SortedSetPutElementResponse.Error}.
   */
  public CompletableFuture<SortedSetPutElementResponse> sortedSetPutElement(
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
   * @return Future containing the result of the put element operation: {@link
   *     SortedSetPutElementResponse.Success} or {@link SortedSetPutElementResponse.Error}.
   */
  public CompletableFuture<SortedSetPutElementResponse> sortedSetPutElement(
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
   * @return Future containing the result of the put element operation: {@link
   *     SortedSetPutElementResponse.Success} or {@link SortedSetPutElementResponse.Error}.
   */
  public CompletableFuture<SortedSetPutElementResponse> sortedSetPutElement(
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
   * @return Future containing the result of the put element operation: {@link
   *     SortedSetPutElementResponse.Success} or {@link SortedSetPutElementResponse.Error}.
   */
  public CompletableFuture<SortedSetPutElementResponse> sortedSetPutElement(
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
   * @return Future containing the result of the put elements operation: {@link
   *     SortedSetPutElementsResponse.Success} or {@link SortedSetPutElementsResponse.Error}.
   */
  public CompletableFuture<SortedSetPutElementsResponse> sortedSetPutElements(
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
   * @return Future containing the result of the put elements operation: {@link
   *     SortedSetPutElementsResponse.Success} or {@link SortedSetPutElementsResponse.Error}.
   */
  public CompletableFuture<SortedSetPutElementsResponse> sortedSetPutElements(
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
   * @return Future containing the result of the put elements operation: {@link
   *     SortedSetPutElementsResponse.Success} or {@link SortedSetPutElementsResponse.Error}.
   */
  public CompletableFuture<SortedSetPutElementsResponse> sortedSetPutElementsByteArray(
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
   * @return Future containing the result of the put elements operation: {@link
   *     SortedSetPutElementsResponse.Success} or {@link SortedSetPutElementsResponse.Error}.
   */
  public CompletableFuture<SortedSetPutElementsResponse> sortedSetPutElementsByteArray(
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
   * @return Future containing the result of the put elements operation: {@link
   *     SortedSetPutElementsResponse.Success} or {@link SortedSetPutElementsResponse.Error}.
   */
  public CompletableFuture<SortedSetPutElementsResponse> sortedSetPutElements(
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
   * @return Future containing the result of the put elements operation: {@link
   *     SortedSetPutElementsResponse.Success} or {@link SortedSetPutElementsResponse.Error}.
   */
  public CompletableFuture<SortedSetPutElementsResponse> sortedSetPutElements(
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
   * @return Future containing the result of the fetch operation: {@link
   *     SortedSetFetchResponse.Hit}, {@link SortedSetFetchResponse.Miss}, or {@link
   *     SortedSetFetchResponse.Error}.
   */
  public CompletableFuture<SortedSetFetchResponse> sortedSetFetchByRank(
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
   * @return Future containing the result of the fetch operation: {@link
   *     SortedSetFetchResponse.Hit}, {@link SortedSetFetchResponse.Miss}, or {@link
   *     SortedSetFetchResponse.Error}.
   */
  public CompletableFuture<SortedSetFetchResponse> sortedSetFetchByRank(
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
   * @return Future containing the result of the fetch operation: {@link
   *     SortedSetFetchResponse.Hit}, {@link SortedSetFetchResponse.Miss}, or {@link
   *     SortedSetFetchResponse.Error}.
   */
  public CompletableFuture<SortedSetFetchResponse> sortedSetFetchByScore(
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
   * @return Future containing the result of the fetch operation: {@link
   *     SortedSetFetchResponse.Hit}, {@link SortedSetFetchResponse.Miss}, or {@link
   *     SortedSetFetchResponse.Error}.
   */
  public CompletableFuture<SortedSetFetchResponse> sortedSetFetchByScore(
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
   * @return Future containing the result of the fetch operation: {@link
   *     SortedSetFetchResponse.Hit}, {@link SortedSetFetchResponse.Miss}, or {@link
   *     SortedSetFetchResponse.Error}.
   */
  public CompletableFuture<SortedSetFetchResponse> sortedSetFetchByScore(
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
   * @return Future containing the result of the get rank operation: {@link
   *     SortedSetGetRankResponse.Hit}, {@link SortedSetGetRankResponse.Miss}, or {@link
   *     SortedSetGetRankResponse.Error}.
   */
  public CompletableFuture<SortedSetGetRankResponse> sortedSetGetRank(
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
   * @return Future containing the result of the get rank operation: {@link
   *     SortedSetGetRankResponse.Hit}, {@link SortedSetGetRankResponse.Miss}, or {@link
   *     SortedSetGetRankResponse.Error}.
   */
  public CompletableFuture<SortedSetGetRankResponse> sortedSetGetRank(
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
   * @return Future containing the result of the get score operation: {@link
   *     SortedSetGetScoreResponse.Hit}, {@link SortedSetGetScoreResponse.Miss}, or {@link
   *     SortedSetGetScoreResponse.Error}.
   */
  public CompletableFuture<SortedSetGetScoreResponse> sortedSetGetScore(
      @Nonnull String cacheName, @Nonnull String sortedSetName, @Nonnull String value) {
    return scsDataClient.sortedSetGetScore(cacheName, sortedSetName, value);
  }

  /**
   * Look up the score of an element in a sorted set.
   *
   * @param cacheName - The cache containing the sorted set.
   * @param sortedSetName - The sorted set to fetch from.
   * @param value - The value whose score we are retrieving.
   * @return Future containing the result of the get score operation: {@link
   *     SortedSetGetScoreResponse.Hit}, {@link SortedSetGetScoreResponse.Miss}, or {@link
   *     SortedSetGetScoreResponse.Error}.
   */
  public CompletableFuture<SortedSetGetScoreResponse> sortedSetGetScore(
      @Nonnull String cacheName, @Nonnull String sortedSetName, @Nonnull byte[] value) {
    return scsDataClient.sortedSetGetScore(cacheName, sortedSetName, value);
  }

  /**
   * Look up the scores of elements in a sorted set.
   *
   * @param cacheName - The cache containing the sorted set.
   * @param sortedSetName - The sorted set to fetch from.
   * @param values - The values whose scores we are retrieving.
   * @return Future containing the result of the get scores operation: {@link
   *     SortedSetGetScoresResponse.Hit}, {@link SortedSetGetScoresResponse.Miss}, or {@link
   *     SortedSetGetScoresResponse.Error}.
   */
  public CompletableFuture<SortedSetGetScoresResponse> sortedSetGetScores(
      @Nonnull String cacheName, @Nonnull String sortedSetName, @Nonnull Iterable<String> values) {
    return scsDataClient.sortedSetGetScores(cacheName, sortedSetName, values);
  }

  /**
   * Look up the scores of elements in a sorted set.
   *
   * @param cacheName - The cache containing the sorted set.
   * @param sortedSetName - The sorted set to fetch from.
   * @param values - The values whose scores we are retrieving.
   * @return Future containing the result of the get scores operation: {@link
   *     SortedSetGetScoresResponse.Hit}, {@link SortedSetGetScoresResponse.Miss}, or {@link
   *     SortedSetGetScoresResponse.Error}.
   */
  public CompletableFuture<SortedSetGetScoresResponse> sortedSetGetScoresByteArray(
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
   * @return Future containing the result of the increment operation: {@link
   *     SortedSetIncrementScoreResponse.Success} or {@link SortedSetIncrementScoreResponse.Error}.
   */
  public CompletableFuture<SortedSetIncrementScoreResponse> sortedSetIncrementScore(
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
   * @return Future containing the result of the increment operation: {@link
   *     SortedSetIncrementScoreResponse.Success} or {@link SortedSetIncrementScoreResponse.Error}.
   */
  public CompletableFuture<SortedSetIncrementScoreResponse> sortedSetIncrementScore(
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
   * @return Future containing the result of the increment operation: {@link
   *     SortedSetIncrementScoreResponse.Success} or {@link SortedSetIncrementScoreResponse.Error}.
   */
  public CompletableFuture<SortedSetIncrementScoreResponse> sortedSetIncrementScore(
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
   * @return Future containing the result of the increment operation: {@link
   *     SortedSetIncrementScoreResponse.Success} or {@link SortedSetIncrementScoreResponse.Error}.
   */
  public CompletableFuture<SortedSetIncrementScoreResponse> sortedSetIncrementScore(
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
   * @return Future containing the result of the remove operation: {@link
   *     SortedSetRemoveElementResponse.Success} or {@link SortedSetRemoveElementResponse.Error}.
   */
  public CompletableFuture<SortedSetRemoveElementResponse> sortedSetRemoveElement(
      @Nonnull String cacheName, @Nonnull String sortedSetName, @Nonnull String value) {
    return scsDataClient.sortedSetRemoveElement(cacheName, sortedSetName, value);
  }

  /**
   * Remove an element from a sorted set
   *
   * @param cacheName - The cache containing the sorted set.
   * @param sortedSetName - The sorted set to remove from.
   * @param value - The value of the element to remove from the set.
   * @return Future containing the result of the remove operation: {@link
   *     SortedSetRemoveElementResponse.Success} or {@link SortedSetRemoveElementResponse.Error}.
   */
  public CompletableFuture<SortedSetRemoveElementResponse> sortedSetRemoveElement(
      @Nonnull String cacheName, @Nonnull String sortedSetName, @Nonnull byte[] value) {
    return scsDataClient.sortedSetRemoveElement(cacheName, sortedSetName, value);
  }

  /**
   * Remove elements from a sorted set
   *
   * @param cacheName - The cache containing the sorted set.
   * @param sortedSetName - The sorted set to remove from.
   * @param values - The values of elements to remove from the set.
   * @return Future containing the result of the remove operation: {@link
   *     SortedSetRemoveElementsResponse.Success} or {@link SortedSetRemoveElementsResponse.Error}.
   */
  public CompletableFuture<SortedSetRemoveElementsResponse> sortedSetRemoveElements(
      @Nonnull String cacheName, @Nonnull String sortedSetName, @Nonnull Iterable<String> values) {
    return scsDataClient.sortedSetRemoveElements(cacheName, sortedSetName, values);
  }

  /**
   * Remove elements from a sorted set
   *
   * @param cacheName - The cache containing the sorted set.
   * @param sortedSetName - The sorted set to remove from.
   * @param values - The values of the elements to remove from the set.
   * @return Future containing the result of the remove operation: {@link
   *     SortedSetRemoveElementsResponse.Success} or {@link SortedSetRemoveElementsResponse.Error}.
   */
  public CompletableFuture<SortedSetRemoveElementsResponse> sortedSetRemoveElementsByteArray(
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
   *     ListConcatenateBackResponse.Success} or {@link ListConcatenateBackResponse.Error}.
   */
  public CompletableFuture<ListConcatenateBackResponse> listConcatenateBack(
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
   *     ListConcatenateBackResponse.Success} or {@link ListConcatenateBackResponse.Error}.
   */
  public CompletableFuture<ListConcatenateBackResponse> listConcatenateBack(
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
   *     ListConcatenateBackResponse.Success} or {@link ListConcatenateBackResponse.Error}.
   */
  public CompletableFuture<ListConcatenateBackResponse> listConcatenateBack(
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
   *     ListConcatenateBackResponse.Success} or {@link ListConcatenateBackResponse.Error}.
   */
  public CompletableFuture<ListConcatenateBackResponse> listConcatenateBackByteArray(
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
   *     ListConcatenateBackResponse.Success} or {@link ListConcatenateBackResponse.Error}.
   */
  public CompletableFuture<ListConcatenateBackResponse> listConcatenateBackByteArray(
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
   *     ListConcatenateBackResponse.Success} or {@link ListConcatenateBackResponse.Error}.
   */
  public CompletableFuture<ListConcatenateBackResponse> listConcatenateBackByteArray(
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
   *     ListConcatenateFrontResponse.Success} or {@link ListConcatenateFrontResponse.Error}.
   */
  public CompletableFuture<ListConcatenateFrontResponse> listConcatenateFront(
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
   *     ListConcatenateFrontResponse.Success} or {@link ListConcatenateFrontResponse.Error}.
   */
  public CompletableFuture<ListConcatenateFrontResponse> listConcatenateFront(
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
   *     ListConcatenateFrontResponse.Success} or {@link ListConcatenateFrontResponse.Error}.
   */
  public CompletableFuture<ListConcatenateFrontResponse> listConcatenateFront(
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
   *     ListConcatenateFrontResponse.Success} or {@link ListConcatenateFrontResponse.Error}.
   */
  public CompletableFuture<ListConcatenateFrontResponse> listConcatenateFrontByteArray(
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
   *     ListConcatenateFrontResponse.Success} or {@link ListConcatenateFrontResponse.Error}.
   */
  public CompletableFuture<ListConcatenateFrontResponse> listConcatenateFrontByteArray(
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
   *     ListConcatenateFrontResponse.Success} or {@link ListConcatenateFrontResponse.Error}.
   */
  public CompletableFuture<ListConcatenateFrontResponse> listConcatenateFrontByteArray(
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
   *     ListFetchResponse.Hit} containing the fetched data, {@link ListFetchResponse.Miss} if no
   *     data was found, or {@link ListFetchResponse.Error}.
   */
  public CompletableFuture<ListFetchResponse> listFetch(
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
   *     ListFetchResponse.Hit} containing the fetched data, {@link ListFetchResponse.Miss} if no
   *     data was found, or {@link ListFetchResponse.Error}.
   */
  public CompletableFuture<ListFetchResponse> listFetch(
      @Nonnull String cacheName, @Nonnull String listName) {
    return scsDataClient.listFetch(cacheName, listName, null, null);
  }

  /**
   * Fetches the length of a list.
   *
   * @param cacheName The cache containing the list.
   * @param listName The list to measure.
   * @return Future containing the result of the list length back operation: {@link
   *     ListLengthResponse.Hit} containing the length, {@link ListLengthResponse.Miss} if no list
   *     was found, or {@link ListLengthResponse.Error}.
   */
  public CompletableFuture<ListLengthResponse> listLength(
      @Nonnull String cacheName, @Nonnull String listName) {
    return scsDataClient.listLength(cacheName, listName);
  }

  /**
   * Fetches and removes the element from the back of a list.
   *
   * @param cacheName The cache containing the list.
   * @param listName The list to fetch the value from.
   * @return Future containing the result of the list pop back operation: {@link
   *     ListPopBackResponse.Hit} containing the element, {@link ListPopBackResponse.Miss} if no
   *     list was found, or {@link ListPopBackResponse.Error}.
   */
  public CompletableFuture<ListPopBackResponse> listPopBack(
      @Nonnull String cacheName, @Nonnull String listName) {
    return scsDataClient.listPopBack(cacheName, listName);
  }

  /**
   * Fetches and removes the element from the front of a list.
   *
   * @param cacheName The cache containing the list.
   * @param listName The list to fetch the value from.
   * @return Future containing the result of the list pop front operation: {@link
   *     ListPopFrontResponse.Hit} containing the element, {@link ListPopFrontResponse.Miss} if no
   *     list was found, or {@link ListPopFrontResponse.Error}.
   */
  public CompletableFuture<ListPopFrontResponse> listPopFront(
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
   *     ListPushBackResponse.Success} or {@link ListPushBackResponse.Error}.
   */
  public CompletableFuture<ListPushBackResponse> listPushBack(
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
   *     ListPushBackResponse.Success} or {@link ListPushBackResponse.Error}.
   */
  public CompletableFuture<ListPushBackResponse> listPushBack(
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
   *     ListPushBackResponse.Success} or {@link ListPushBackResponse.Error}.
   */
  public CompletableFuture<ListPushBackResponse> listPushBack(
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
   *     ListPushBackResponse.Success} or {@link ListPushBackResponse.Error}.
   */
  public CompletableFuture<ListPushBackResponse> listPushBack(
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
   *     ListPushBackResponse.Success} or {@link ListPushBackResponse.Error}.
   */
  public CompletableFuture<ListPushBackResponse> listPushBack(
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
   *     ListPushBackResponse.Success} or {@link ListPushBackResponse.Error}.
   */
  public CompletableFuture<ListPushBackResponse> listPushBack(
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
   *     ListPushFrontResponse.Success} or {@link ListPushFrontResponse.Error}.
   */
  public CompletableFuture<ListPushFrontResponse> listPushFront(
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
   *     ListPushFrontResponse.Success} or {@link ListPushFrontResponse.Error}.
   */
  public CompletableFuture<ListPushFrontResponse> listPushFront(
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
   *     ListPushFrontResponse.Success} or {@link ListPushFrontResponse.Error}.
   */
  public CompletableFuture<ListPushFrontResponse> listPushFront(
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
   *     ListPushFrontResponse.Success} or {@link ListPushFrontResponse.Error}.
   */
  public CompletableFuture<ListPushFrontResponse> listPushFront(
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
   *     ListPushFrontResponse.Success} or {@link ListPushFrontResponse.Error}.
   */
  public CompletableFuture<ListPushFrontResponse> listPushFront(
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
   *     ListPushFrontResponse.Success} or {@link ListPushFrontResponse.Error}.
   */
  public CompletableFuture<ListPushFrontResponse> listPushFront(
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
   *     ListRemoveValueResponse.Success} or {@link ListRemoveValueResponse.Error}.
   */
  public CompletableFuture<ListRemoveValueResponse> listRemoveValue(
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
   *     ListRemoveValueResponse.Success} or {@link ListRemoveValueResponse.Error}.
   */
  public CompletableFuture<ListRemoveValueResponse> listRemoveValue(
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
   *     ListRetainResponse.Success} or {@link ListRetainResponse.Error}.
   */
  public CompletableFuture<ListRetainResponse> listRetain(
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
   *     DictionaryFetchResponse.Hit} containing the dictionary, {@link
   *     DictionaryFetchResponse.Miss} if no dictionary was found, or {@link
   *     DictionaryFetchResponse.Error}.
   */
  public CompletableFuture<DictionaryFetchResponse> dictionaryFetch(
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
   *     DictionarySetFieldResponse.Success} or {@link DictionarySetFieldResponse.Error}.
   */
  public CompletableFuture<DictionarySetFieldResponse> dictionarySetField(
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
   *     DictionarySetFieldResponse.Success} or {@link DictionarySetFieldResponse.Error}.
   */
  public CompletableFuture<DictionarySetFieldResponse> dictionarySetField(
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
   *     DictionarySetFieldResponse.Success} or {@link DictionarySetFieldResponse.Error}.
   */
  public CompletableFuture<DictionarySetFieldResponse> dictionarySetField(
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
   *     DictionarySetFieldResponse.Success} or {@link DictionarySetFieldResponse.Error}.
   */
  public CompletableFuture<DictionarySetFieldResponse> dictionarySetField(
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
   *     DictionarySetFieldsResponse.Success} or {@link DictionarySetFieldsResponse.Error}.
   */
  public CompletableFuture<DictionarySetFieldsResponse> dictionarySetFields(
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
   *     DictionarySetFieldsResponse.Success} or {@link DictionarySetFieldsResponse.Error}.
   */
  public CompletableFuture<DictionarySetFieldsResponse> dictionarySetFields(
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
   *     DictionarySetFieldsResponse.Success} or {@link DictionarySetFieldsResponse.Error}.
   */
  public CompletableFuture<DictionarySetFieldsResponse> dictionarySetFieldsStringBytes(
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
   *     DictionarySetFieldsResponse.Success} or {@link DictionarySetFieldsResponse.Error}.
   */
  public CompletableFuture<DictionarySetFieldsResponse> dictionarySetFieldsStringBytes(
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
   *     DictionaryGetFieldResponse.Hit} containing the field and value, {@link
   *     DictionaryGetFieldResponse.Miss} if the field was not found in the dictionary, or {@link
   *     DictionaryGetFieldResponse.Error}.
   */
  public CompletableFuture<DictionaryGetFieldResponse> dictionaryGetField(
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
   *     DictionaryGetFieldsResponse.Hit} containing the fields, values, and a per-field {@link
   *     DictionaryGetFieldResponse}, {@link DictionaryGetFieldsResponse.Miss} if none of the fields
   *     were found in the dictionary, or {@link DictionaryGetFieldsResponse.Error}.
   */
  public CompletableFuture<DictionaryGetFieldsResponse> dictionaryGetFields(
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
   *     DictionaryIncrementResponse.Success} or {@link DictionaryIncrementResponse.Error}.
   */
  public CompletableFuture<DictionaryIncrementResponse> dictionaryIncrement(
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
   *     DictionaryIncrementResponse.Success} or {@link DictionaryIncrementResponse.Error}.
   */
  public CompletableFuture<DictionaryIncrementResponse> dictionaryIncrement(
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
   *     DictionaryRemoveFieldResponse.Success} or {@link DictionaryRemoveFieldResponse.Error}.
   */
  public CompletableFuture<DictionaryRemoveFieldResponse> dictionaryRemoveField(
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
   *     DictionaryRemoveFieldsResponse.Success} or {@link DictionaryRemoveFieldsResponse.Error}.
   */
  public CompletableFuture<DictionaryRemoveFieldsResponse> dictionaryRemoveFields(
      @Nonnull String cacheName, @Nonnull String dictionaryName, @Nonnull Iterable<String> fields) {
    return scsDataClient.dictionaryRemoveFields(cacheName, dictionaryName, fields);
  }

  @Override
  public void close() {
    scsControlClient.close();
    scsDataClient.close();
  }
}
