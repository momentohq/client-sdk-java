package momento.sdk.responses.cache.set;

import momento.sdk.exceptions.SdkException;

/** Response for a set add elements operation */
public interface SetAddElementsResponse {

  /** A successful set add elements operation. */
  class Success implements SetAddElementsResponse {}

  /**
   * A failed set add elements operation. The response itself is an exception, so it can be directly
   * thrown, or the cause of the error can be retrieved with {@link #getCause()}. The message is a
   * copy of the message of the cause.
   */
  class Error extends SdkException implements SetAddElementsResponse {

    /**
     * Constructs a set add elements error with a cause.
     *
     * @param cause the cause.
     */
    public Error(SdkException cause) {
      super(cause);
    }
  }
}
