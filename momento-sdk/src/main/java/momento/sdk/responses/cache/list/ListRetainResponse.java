package momento.sdk.responses.cache.list;

import momento.sdk.exceptions.SdkException;

/**
 * Parent response type for a list retain value request. The response object is resolved to a
 * type-safe object of one of the following subtypes:
 *
 * <p>{Success}, {Error}
 */
public interface ListRetainResponse {

  /** A successful list retain value operation. */
  class Success implements ListRetainResponse {}

  /**
   * A failed list retain value operation. The response itself is an exception, so it can be
   * directly thrown, or the cause of the error can be retrieved with {@link #getClass()} ()}. The
   * message is a copy of the message of the cause.
   */
  class Error extends SdkException implements ListRetainResponse {

    /**
     * Constructs a list retain value error with a cause.
     *
     * @param cause the cause.
     */
    public Error(SdkException cause) {
      super(cause);
    }
  }
}
