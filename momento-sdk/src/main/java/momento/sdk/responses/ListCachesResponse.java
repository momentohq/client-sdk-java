package momento.sdk.responses;

import java.util.List;
import java.util.stream.Collectors;
import momento.sdk.exceptions.SdkException;

/** Response for a list caches operation. */
public interface ListCachesResponse {

  /** A successful list caches operation. Contains the discovered caches. */
  class Success implements ListCachesResponse {
    private final List<CacheInfo> caches;

    /**
     * Constructs a list caches success with a list of found caches.
     *
     * @param caches the retrieved caches.
     */
    public Success(List<CacheInfo> caches) {
      this.caches = caches;
    }

    /**
     * Gets a list of metadata about the caches.
     *
     * @return The metadata list.
     */
    public List<CacheInfo> getCaches() {
      return caches;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Limits the caches to 5 to bound the size of the string.
     */
    @Override
    public String toString() {
      return super.toString()
          + ": "
          + getCaches().stream()
              .map(CacheInfo::name)
              .limit(5)
              .collect(Collectors.joining("\", \"", "\"", "\"..."));
    }
  }

  /**
   * A failed list caches operation. The response itself is an exception, so it can be directly
   * thrown, or the cause of the error can be retrieved with {@link #getCause()}. The message is a
   * copy of the message of the cause.
   */
  class Error extends SdkException implements ListCachesResponse {

    /**
     * Constructs a list caches error with a cause.
     *
     * @param cause the cause.
     */
    public Error(SdkException cause) {
      super(cause);
    }
  }
}
