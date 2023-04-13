package momento.sdk.messages;

import momento.sdk.exceptions.SdkException;

/** Response for a sorted set put element operation */
public interface CacheSortedSetPutElementResponse {

  /** A successful sorted set put element operation. */
  class Success implements CacheSortedSetPutElementResponse {}

  /**
   * A failed sorted set put element operation. The response itself is an exception, so it can be
   * directly thrown, or the cause of the error can be retrieved with {@link #getCause()}. The
   * message is a copy of the message of the cause.
   */
  class Error extends SdkException implements CacheSortedSetPutElementResponse {

    /**
     * Constructs a sorted set put element error with a cause.
     *
     * @param cause the cause.
     */
    public Error(SdkException cause) {
      super(cause);
    }
  }
}
