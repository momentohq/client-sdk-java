package momento.sdk.responses.cache.control;

import momento.sdk.exceptions.SdkException;

/** Response for a delete cache operation */
public interface CacheDeleteResponse {

  /** A successful delete cache operation. */
  class Success implements CacheDeleteResponse {}

  /**
   * A failed delete cache operation. The response itself is an exception, so it can be directly
   * thrown, or the cause of the error can be retrieved with {@link #getCause()}. The message is a
   * copy of the message of the cause.
   */
  class Error extends SdkException implements CacheDeleteResponse {

    /**
     * Constructs a delete cache error with a cause.
     *
     * @param cause the cause.
     */
    public Error(SdkException cause) {
      super(cause);
    }
  }
}
