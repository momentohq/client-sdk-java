package momento.sdk.responses.cache.control;

import momento.sdk.exceptions.SdkException;

/** Response for a flush cache operation */
public interface CacheFlushResponse {

  /** A successful flush cache operation. */
  class Success implements CacheFlushResponse {}

  /**
   * A failed flush cache operation. The response itself is an exception, so it can be directly
   * thrown, or the cause of the error can be retrieved with {@link #getCause()}. The message is a
   * copy of the message of the cause.
   */
  class Error extends SdkException implements CacheFlushResponse {

    /**
     * Constructs a cache flush error with a cause.
     *
     * @param cause the cause.
     */
    public Error(SdkException cause) {
      super(cause);
    }
  }
}
