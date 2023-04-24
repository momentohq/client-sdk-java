package momento.sdk.responses.cache.sortedset;

import momento.sdk.exceptions.SdkException;

/** Response for a sorted set remove element operation */
public interface SortedSetRemoveElementResponse {

  /** A successful sorted set remove element operation. */
  class Success implements SortedSetRemoveElementResponse {}

  /**
   * A failed sorted set remove element operation. The response itself is an exception, so it can be
   * directly thrown, or the cause of the error can be retrieved with {@link #getCause()}. The
   * message is a copy of the message of the cause.
   */
  class Error extends SdkException implements SortedSetRemoveElementResponse {

    /**
     * Constructs a sorted set remove element error with a cause.
     *
     * @param cause the cause.
     */
    public Error(SdkException cause) {
      super(cause);
    }
  }
}
