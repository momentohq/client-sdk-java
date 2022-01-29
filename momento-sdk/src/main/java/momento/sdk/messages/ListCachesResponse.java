package momento.sdk.messages;

import java.util.List;
import java.util.Optional;
import momento.sdk.SimpleCacheClient;

/** Response object for list of caches. */
public final class ListCachesResponse {

  private final List<CacheInfo> caches;
  private final Optional<String> nextPageToken;

  public ListCachesResponse(List<CacheInfo> caches, Optional<String> nextPageToken) {
    this.caches = caches;
    this.nextPageToken = nextPageToken;
  }

  public List<CacheInfo> caches() {
    return caches;
  }

  /**
   * Next Page Token returned by Simple Cache Service along with the list of caches.
   *
   * <p>If nextPageToken().isPresent(), then this token must be provided in the next call to
   * continue paginating through the list. This is done by setting the value in {@link
   * SimpleCacheClient#listCaches(Optional)}
   *
   * <p>When not present, there are no more caches to return.
   */
  public Optional<String> nextPageToken() {
    return nextPageToken;
  }
}
