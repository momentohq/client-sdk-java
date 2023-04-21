package momento.sdk.messages;

import momento.sdk.exceptions.SdkException;

/** Response for a dictionary set fields operation */
public interface CacheDictionarySetFieldsResponse {
  /** A successful dictionary set fields operation. */
  class Success implements CacheDictionarySetFieldsResponse {}

  /**
   * A failed dictionary set fields operation. The response itself is an exception, so it can be
   * directly thrown, or the cause of the error can be retrieved with {@link #getCause()}. The
   * message is a copy of the message of the cause.
   */
  class Error extends SdkException implements CacheDictionarySetFieldsResponse {

    /**
     * Constructs a dictionary set fields error with a cause.
     *
     * @param cause The cause.
     */
    public Error(SdkException cause) {
      super(cause);
    }
  }
}
