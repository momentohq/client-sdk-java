package momento.sdk.responses.cache.sortedset;

import momento.sdk.exceptions.SdkException;

/** Response for a sorted set remove elements operation */
public interface SortedSetRemoveElementsResponse {

  /** A successful sorted set remove elements operation. */
  class Success implements SortedSetRemoveElementsResponse {}

  /**
   * A failed sorted set remove elements operation. The response itself is an exception, so it can
   * be directly thrown, or the cause of the error can be retrieved with {@link #getCause()}. The
   * message is a copy of the message of the cause.
   */
  class Error extends SdkException implements SortedSetRemoveElementsResponse {

    /**
     * Constructs a sorted set remove elements error with a cause.
     *
     * @param cause the cause.
     */
    public Error(SdkException cause) {
      super(cause);
    }
  }
}
