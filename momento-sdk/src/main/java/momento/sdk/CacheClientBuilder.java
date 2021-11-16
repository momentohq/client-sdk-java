package momento.sdk;

import momento.sdk.exceptions.CacheNotFoundException;
import momento.sdk.exceptions.ClientSdkException;

/** Build a {@link Cache} */
public final class CacheClientBuilder {

  private final Momento momento;
  private final String authToken;
  private final String cacheName;
  private final int defaultItemTtlSeconds;
  private final String endpoint;
  private boolean createIfDoesntExist;

  CacheClientBuilder(
      Momento momento,
      String authToken,
      String cacheName,
      int defaultItemTtlSeconds,
      String endpoint) {
    this.momento = momento;
    this.authToken = authToken;
    this.cacheName = cacheName;
    this.defaultItemTtlSeconds = defaultItemTtlSeconds;
    this.endpoint = endpoint;
  }

  /** Signal the builder to create a new Cache if one with the given name doesn't exist. */
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
    Cache cache = null;
    try {
      cache = new Cache(authToken, cacheName, endpoint, defaultItemTtlSeconds);
      return cache.connect();
    } catch (CacheNotFoundException e) {
      if (!createIfDoesntExist) {
        throw e;
      }
    }

    // Create since the cache is not found and the request is to create it.
    momento.createCache(cacheName);
    // Use the same cache object as previously constructed.
    return cache.connect();
  }
}
