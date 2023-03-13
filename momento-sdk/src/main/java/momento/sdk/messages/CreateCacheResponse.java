package momento.sdk.messages;

import momento.sdk.exceptions.SdkException;

/** Response for a create cache operation */
public interface CreateCacheResponse {

  /** A successful create cache operation. */
  class Success implements CreateCacheResponse {}

  /**
   * A failed create cache operation. The response itself is an exception, so it can be directly
   * thrown, or the cause of the error can be retrieved with {@link #getCause()}. The message is a
   * copy of the message of the cause.
   */
  class Error extends SdkException implements CreateCacheResponse {

    public Error(SdkException cause) {
      super(cause.getMessage(), cause);
    }
  }
}
