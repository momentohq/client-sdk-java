package momento.sdk.messages;

import momento.sdk.exceptions.SdkException;

/** Response for a set add element operation */
public interface CacheSetAddElementResponse {

  /** A successful set add element operation. */
  class Success implements CacheSetAddElementResponse {}

  /**
   * A failed set add element operation. The response itself is an exception, so it can be directly
   * thrown, or the cause of the error can be retrieved with {@link #getCause()}. The message is a
   * copy of the message of the cause.
   */
  class Error extends SdkException implements CacheSetAddElementResponse {

    /**
     * Constructs a set add element error with a cause.
     *
     * @param cause the cause.
     */
    public Error(SdkException cause) {
      super(cause);
    }
  }
}
