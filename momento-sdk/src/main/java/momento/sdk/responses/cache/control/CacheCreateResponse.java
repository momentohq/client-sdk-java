package momento.sdk.responses.cache.control;

import momento.sdk.exceptions.SdkException;

/** Response for a create cache operation */
public interface CacheCreateResponse {

  /** A successful create cache operation. */
  class Success implements CacheCreateResponse {}

  /**
   * A failed create cache operation. The response itself is an exception, so it can be directly
   * thrown, or the cause of the error can be retrieved with {@link #getCause()}. The message is a
   * copy of the message of the cause.
   */
  class Error extends SdkException implements CacheCreateResponse {

    /**
     * Constructs a cache creation error with a cause.
     *
     * @param cause the cause.
     */
    public Error(SdkException cause) {
      super(cause);
    }
  }
}
