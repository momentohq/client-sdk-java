package momento.sdk.messages;

import momento.sdk.exceptions.SdkException;

/** Response for a set remove elements operation */
public interface CacheSetRemoveElementsResponse {

  /** A successful set remove elements operation. */
  class Success implements CacheSetRemoveElementsResponse {}

  /**
   * A failed set remove elements operation. The response itself is an exception, so it can be
   * directly thrown, or the cause of the error can be retrieved with {@link #getCause()}. The
   * message is a copy of the message of the cause.
   */
  class Error extends SdkException implements CacheSetRemoveElementsResponse {

    /**
     * Constructs a set remove elements error with a cause.
     *
     * @param cause the cause.
     */
    public Error(SdkException cause) {
      super(cause);
    }
  }
}
