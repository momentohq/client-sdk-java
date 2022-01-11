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

  /** Signal the builder to create a new Cache if one with the given name doesn't exist. */
  // TODO: Should we drop this? The reason is as follows: (Will remove this after PR discussion)
  // In normal course of operation, I do expect customers running production code to be able to just
  // inject Momento in
  // in their application and then just always do get a Cache entity (since it is cheap) and then
  // perform operations.
  //
  // Following class is for demonstration purposes
  //
  //  class CacheManager {
  //    private final Momento momento;
  //
  //    @Inject
  //    public CacheManager(Momento momento) {
  //      this.momento = momento;
  //    }
  //
  //    public void addToCache(String cacheName, CacheData cacheData) {
  //      Cache cache = momento.cacheBuilder(cacheName, cacheData.ttlSeconds).build();
  //      cache.set(cacheData.key, cacheData.value);
  //    }
  //
  //    public Data getData(String cacheName, String cacheKey) {
  //      Cache cache = momento.cacheBuilder(cacheName, cacheData.ttlSeconds).build();
  //      retrun Data.create(cache.get(cacheKey));
  //    }
  //  }
  //
  // We would never want customers to use createCacheIfDoesntExist. Although it ensures that Cache
  // will always be
  // present, adding that option to production code will always result in a GET first before any
  // other operation thus
  // incurring additional latency.
  //
  // The flipside to removing this would potentially require customers -
  // 1. Create their caches before hand
  // 2. Maintain a registry of caches returned by Momento
  // 3. Add custom code to catch NotFoundExceptions and then create cache
  // 4. It will endup making demonstration code longer.
  //
  // An intermediate way may be to keep this method and add good comments around why this shouldn't
  // be done in
  // production code.
  //
  // Other option is to add this optional flag to get or set operations.
  //
  // So, we take this parameter on the builder but we only create a cache if we encounter
  // CacheNotFound on the requested
  // operation. There is still some race so some calls may end up taking longer around the time
  // cache doesn't exist. But
  // will eventually even out. May also endup adding additional risk to control plan.
  //
  // e.g. code
  //  public void set(String key, String value) {
  //    try {
  //      scsGrpcClient.set(key, value, ttl)
  //    } catch(CacheNotFoundException e) {
  //      if (createIfDoesntExist) {
  //        try {
  //          momento.createCache(cacheName);
  //        } catch (CacheAlreadyExistsException) {
  //          // ignore
  //        }
  //        set(key, value);
  //      }
  //    }
  //  }
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
