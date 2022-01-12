package momento.sdk;

import momento.sdk.exceptions.CacheNotFoundException;
import momento.sdk.exceptions.ClientSdkException;

/** Build a {@link Cache} */
public final class CacheClientBuilder {

  private final Momento momento;
  private final String cacheName;
  private final int defaultItemTtlSeconds;
  private boolean createIfDoesntExist;

  CacheClientBuilder(Momento momento, String cacheName, int defaultItemTtlSeconds) {
    this.momento = momento;
    this.cacheName = cacheName;
    this.defaultItemTtlSeconds = defaultItemTtlSeconds;
  }

  /**
   * Signal the builder to create a new Cache if one with the given name doesn't exist. This
   * parameter should be used judiciously. Using this parameter, makes a test call to the backend to
   * determine if the cache exists or not. So,if builder is used in a hot path with this parameter
   * SET, additional latencies will be incurred. For production use cases, the recommendation is to
   * create caches using {@link Momento#createCache(String)} operation prior to performing
   * operations.
   */
  public CacheClientBuilder createCacheIfDoesntExist() {
    this.createIfDoesntExist = true;
    return this;
  }

  /** Builds {@link Cache} client based on the properties set on the builder. */
  public Cache build() {
    if (defaultItemTtlSeconds <= 0) {
      throw new ClientSdkException("Item's time to live in Cache must be a positive integer.");
    }
    Momento.checkCacheNameValid(cacheName);
    Cache cache = new Cache(cacheName, defaultItemTtlSeconds, momento.getScsClient());
    if (createIfDoesntExist) {
      try {
        cache.connect();
      } catch (CacheNotFoundException e) {
        momento.createCache(cacheName);
      }
      // Test connection
      cache.connect();
    }
    return cache;
  }
}
