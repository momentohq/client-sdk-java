package momento.sdk.messages;

import momento.sdk.exceptions.SdkException;
import momento.sdk.exceptions.WrappedSdkException;

/** Response for a delete cache operation */
public interface DeleteCacheResponse {

  /** A successful delete cache operation. */
  class Success implements DeleteCacheResponse {}

  /**
   * A failed delete cache operation. The response itself is an exception, so it can be directly
   * thrown, or the cause of the error can be retrieved with {@link #getCause()}. The message is a
   * copy of the message of the cause.
   */
  class Error extends WrappedSdkException implements DeleteCacheResponse {

    public Error(SdkException cause) {
      super(cause);
    }
  }
}
