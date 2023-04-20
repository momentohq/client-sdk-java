package momento.sdk.messages;

import momento.sdk.exceptions.SdkException;

/** Response for a dictionary set field operation */
public interface CacheDictionarySetFieldResponse {
  /** A successful dictionary set field operation. */
  class Success implements CacheDictionarySetFieldResponse {}

  /**
   * A failed dictionary set field operation. The response itself is an exception, so it can be
   * directly thrown, or the cause of the error can be retrieved with {@link #getCause()}. The
   * message is a copy of the message of the cause.
   */
  class Error extends SdkException implements CacheDictionarySetFieldResponse {

    /**
     * Constructs a dictionary set field error with a cause.
     *
     * @param cause The cause.
     */
    public Error(SdkException cause) {
      super(cause);
    }
  }
}
