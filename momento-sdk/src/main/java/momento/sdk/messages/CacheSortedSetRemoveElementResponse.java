package momento.sdk.messages;

import momento.sdk.exceptions.SdkException;

/** Response for a sorted set remove element operation */
public interface CacheSortedSetRemoveElementResponse {

  /** A successful sorted set remove element operation. */
  class Success implements CacheSortedSetRemoveElementResponse {}

  /**
   * A failed sorted set remove element operation. The response itself is an exception, so it can be
   * directly thrown, or the cause of the error can be retrieved with {@link #getCause()}. The
   * message is a copy of the message of the cause.
   */
  class Error extends SdkException implements CacheSortedSetRemoveElementResponse {

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
