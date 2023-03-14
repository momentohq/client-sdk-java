package momento.sdk.messages;

import java.util.List;
import momento.sdk.exceptions.SdkException;

/** Response for a list caches operation. */
public interface ListCachesResponse {

  /** A successful list caches operation. Contains the discovered caches. */
  class Success implements ListCachesResponse {
    private final List<CacheInfo> caches;

    public Success(List<CacheInfo> caches) {
      this.caches = caches;
    }

    public List<CacheInfo> getCaches() {
      return caches;
    }
  }

  /**
   * A failed list caches operation. The response itself is an exception, so it can be directly
   * thrown, or the cause of the error can be retrieved with {@link #getCause()}. The message is a
   * copy of the message of the cause.
   */
  class Error extends SdkException implements ListCachesResponse {

    public Error(SdkException cause) {
      super(cause.getMessage(), cause);
    }
  }
}
