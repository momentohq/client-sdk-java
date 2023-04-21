package momento.sdk.responses;

import momento.sdk.exceptions.SdkException;

/** Response for a cache delete operation */
public interface CacheDeleteResponse {

  /** A successful cache delete operation. */
  class Success implements CacheDeleteResponse {}

  /**
   * A failed cache delete operation. The response itself is an exception, so it can be directly
   * thrown, or the cause of the error can be retrieved with {@link #getCause()}. The message is a
   * copy of the message of the cause.
   */
  class Error extends SdkException implements CacheDeleteResponse {

    /**
     * Constructs a cache delete error with a cause.
     *
     * @param cause the cause.
     */
    public Error(SdkException cause) {
      super(cause);
    }
  }
}
