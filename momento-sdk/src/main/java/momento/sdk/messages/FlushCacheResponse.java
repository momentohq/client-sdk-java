package momento.sdk.messages;

import momento.sdk.exceptions.SdkException;
import momento.sdk.exceptions.WrappedSdkException;

/** Response for a flush cache operation */
public interface FlushCacheResponse {

  /** A successful flush cache operation. */
  class Success implements FlushCacheResponse {}

  /**
   * A failed flush cache operation. The response itself is an exception, so it can be directly
   * thrown, or the cause of the error can be retrieved with {@link #getCause()}. The message is a
   * copy of the message of the cause.
   */
  class Error extends WrappedSdkException implements FlushCacheResponse {

    public Error(SdkException cause) {
      super(cause);
    }
  }
}
