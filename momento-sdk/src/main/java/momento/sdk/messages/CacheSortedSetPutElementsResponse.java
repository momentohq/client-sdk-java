package momento.sdk.messages;

import momento.sdk.exceptions.SdkException;

/** Response for a sorted set put elements operation */
public interface CacheSortedSetPutElementsResponse {

  /** A successful sorted set put elements operation. */
  class Success implements CacheSortedSetPutElementsResponse {}

  /**
   * A failed sorted set put elements operation. The response itself is an exception, so it can be
   * directly thrown, or the cause of the error can be retrieved with {@link #getCause()}. The
   * message is a copy of the message of the cause.
   */
  class Error extends SdkException implements CacheSortedSetPutElementsResponse {

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
