package momento.sdk.responses.cache.sortedset;

import momento.sdk.exceptions.SdkException;

/** Response for a sorted set put elements operation */
public interface SortedSetPutElementsResponse {

  /** A successful sorted set put elements operation. */
  class Success implements SortedSetPutElementsResponse {}

  /**
   * A failed sorted set put elements operation. The response itself is an exception, so it can be
   * directly thrown, or the cause of the error can be retrieved with {@link #getCause()}. The
   * message is a copy of the message of the cause.
   */
  class Error extends SdkException implements SortedSetPutElementsResponse {

    /**
     * Constructs a sorted set put elements error with a cause.
     *
     * @param cause the cause.
     */
    public Error(SdkException cause) {
      super(cause);
    }
  }
}
