package momento.sdk.responses.cache.set;

import momento.sdk.exceptions.SdkException;

/** Response for a set remove element operation */
public interface SetRemoveElementResponse {

  /** A successful set remove element operation. */
  class Success implements SetRemoveElementResponse {}

  /**
   * A failed set remove element operation. The response itself is an exception, so it can be
   * directly thrown, or the cause of the error can be retrieved with {@link #getCause()}. The
   * message is a copy of the message of the cause.
   */
  class Error extends SdkException implements SetRemoveElementResponse {

    /**
     * Constructs a set remove element error with a cause.
     *
     * @param cause the cause.
     */
    public Error(SdkException cause) {
      super(cause);
    }
  }
}
