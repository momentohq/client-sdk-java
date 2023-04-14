package momento.sdk.requests;

import java.time.Duration;
import java.util.Optional;
import javax.annotation.Nullable;

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
  private final Duration ttl;
  private final boolean refreshTtl;

  /**
   * Constructs a CollectionTtl.
   *
   * @param ttl The TTL for the collection. If null, the client's default TTL will be used.
   * @param refreshTtl Whether to refresh the ttl when the collection is modified.
   */
  public CollectionTtl(@Nullable Duration ttl, boolean refreshTtl) {
    this.refreshTtl = refreshTtl;
    this.ttl = ttl;
  }

  /**
   * Converts the TTL to seconds if it is present.
   *
   * @return The TTL in seconds or {@link Optional#empty()} if the default TTL will be used.
   */
  public Optional<Long> toSeconds() {
    return Optional.ofNullable(ttl).map(Duration::getSeconds);
  }

  /**
   * Converts the TTL to milliseconds if it is present.
   *
   * @return The TTL in milliseconds or {@link Optional#empty()} if the default TTL will be used.
   */
  public Optional<Long> toMilliseconds() {
    return Optional.ofNullable(ttl).map(Duration::toMillis);
  }

  /**
   * Returns whether the collection TTL will be refreshed.
   *
   * @return Whether the collection TTL will be refreshed.
   */
  public boolean refreshTtl() {
    return this.refreshTtl;
  }

  /**
   * The default way to handle TTLs for collections. The client's default TTL will be used, and the
   * TTL for the collection will be refreshed any time the collection is modified.
   *
   * @return CollectionTtl
   */
  public static CollectionTtl fromCacheTtl() {
    return new CollectionTtl(null, true);
  }

  /**
   * Constructs a CollectionTtl with the specified TTL. The TTL for the collection will be refreshed
   * any time the collection is modified.
   *
   * @param ttl The TTL for the collection. If null, the client's default TTL will be used.
   * @return CollectionTtl
   */
  public static CollectionTtl of(@Nullable Duration ttl) {
    return new CollectionTtl(ttl, true);
  }

  /**
   * Constructs a CollectionTtl with the specified TTL. Will only refresh if the TTL is provided.
   *
   * @param ttl The TTL for the collection. If null, the client's default TTL will be used.
   * @return CollectionTtl
   */
  public static CollectionTtl refreshTtlIfProvided(@Nullable Duration ttl) {
    return new CollectionTtl(ttl, ttl != null);
  }

  /**
   * Copies the CollectionTtl, but it will refresh the TTL when the collection is modified.
   *
   * @return CollectionTtl
   */
  public CollectionTtl withRefreshTtlOnUpdates() {
    return new CollectionTtl(ttl, true);
  }

  /**
   * Copies the CollectionTTL, but the TTL will not be refreshed when the collection is modified.
   * Use this if you want to ensure that your collection expires at the originally specified time,
   * even if you make modifications to the value of the collection.
   *
   * @return CollectionTtl
   */
  public CollectionTtl withNoRefreshTtlOnUpdates() {
    return new CollectionTtl(ttl, false);
  }
}
