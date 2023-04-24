package momento.sdk.responses.cache.set;

import momento.sdk.exceptions.SdkException;

/** Response for a set remove elements operation */
public interface SetRemoveElementsResponse {

  /** A successful set remove elements operation. */
  class Success implements SetRemoveElementsResponse {}

  /**
   * A failed set remove elements operation. The response itself is an exception, so it can be
   * directly thrown, or the cause of the error can be retrieved with {@link #getCause()}. The
   * message is a copy of the message of the cause.
   */
  class Error extends SdkException implements SetRemoveElementsResponse {

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
