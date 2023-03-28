package momento.sdk.requests;

import java.time.Duration;

/**
 * Represents the desired behavior for managing the TTL on collection objects (dictionaries, lists,
 * sets) in your cache. For cache operations that modify a collection, there are a few things to
 * consider. The first time the collection is created, we need to set a TTL on it. For subsequent
 * operations that modify the collection you may choose to update the TTL in order to prolong the
 * life of the cached collection object, or you may choose to leave the TTL unmodified in order to
 * ensure that the collection expires at the original TTL. The default behavior is to refresh the
 * TTL (to prolong the life of the collection) each time it is written. This behavior can be
 * modified by calling CollectionTtl.withNoRefreshTtlOnUpdates(). A null TTL means to use the
 * client's TTL.
 */
public class CollectionTtl {
  private Duration ttlSeconds;
  private boolean refreshTtl;

  public CollectionTtl(Duration ttlSeconds, boolean refreshTtl) {
    this.refreshTtl = refreshTtl;
    this.ttlSeconds = ttlSeconds;
  }

  public Duration ttSeconds() {
    return this.ttlSeconds;
  }

  public long ttMilliseconds() {
    return ttlSeconds.toMillis();
  }

  public boolean refreshTtl() {
    return this.refreshTtl;
  }

  /**
   * The default way to handle TTLs for collections. The client's default TTL will be used, and the
   * TTL for the collection will be refreshed any time the collection is modified.
   *
   * @return {CollectionTtl}
   */
  public static CollectionTtl fromCacheTtl() {
    return new CollectionTtl(null, true);
  }

  /**
   * Constructs a CollectionTtl with the specified TTL. The TTL for the collection will be refreshed
   * any time the collection is modified.
   *
   * @param ttlSeconds
   * @return {CollectionTtl}
   */
  public static CollectionTtl of(Duration ttlSeconds) {
    return new CollectionTtl(ttlSeconds, true);
  }

  /**
   * Constructs a CollectionTtl with the specified TTL. Will only refresh if the TTL is provided.
   *
   * @param ttlSeconds
   * @return {CollectionTtl}
   */
  public static CollectionTtl refreshTtlIfProvided(Duration ttlSeconds) {
    return new CollectionTtl(ttlSeconds, ttlSeconds != null);
  }

  /**
   * Copies the CollectionTtl, but it will refresh the TTL when the collection is modified.
   *
   * @return {CollectionTtl}
   */
  public CollectionTtl withRefreshTtlOnUpdates() {
    return new CollectionTtl(ttlSeconds, true);
  }

  /**
   * Copies the CollectionTTL, but the TTL will not be refreshed when the collection is modified.
   * Use this if you want to ensure that your collection expires at the originally specified time,
   * even if you make modifications to the value of the collection.
   *
   * @return {CollectionTtl}
   */
  public CollectionTtl withNoRefreshTtlOnUpdates() {
    return new CollectionTtl(ttlSeconds, false);
  }
}
