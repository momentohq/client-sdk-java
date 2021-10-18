package momento.sdk;

import momento.sdk.exceptions.CacheAlreadyExistsException;

/**
 * Build a {@link Cache}
 */
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

  /**
   * Signal the builder to create a new Cache if one with the given name doesn't exist.
   * @return
   */
  public CacheClientBuilder createCacheIfDoesntExist() {
    this.createIfDoesntExist = true;
    return this;
  }

  public Cache build() {
    Momento.checkCacheNameValid(cacheName);
    if (createIfDoesntExist) {
      try {
        momento.createCache(cacheName);
      } catch (CacheAlreadyExistsException e) {
      }
    }
    return new Cache(authToken, cacheName, endpoint, defaultItemTtlSeconds);
  }
}
