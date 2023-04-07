package momento.sdk.messages;

import momento.sdk.exceptions.SdkException;

/** Response for a dictionary remove fields operation */
public interface CacheDictionaryRemoveFieldsResponse {
  /** A successful dictionary remove fields operation. */
  class Success implements CacheDictionaryRemoveFieldsResponse {}

  /**
   * A failed dictionary remove fields operation. The response itself is an exception, so it can be
   * directly thrown, or the cause of the error can be retrieved with {@link #getCause()}. The
   * message is a copy of the message of the cause.
   */
  class Error extends SdkException implements CacheDictionaryRemoveFieldsResponse {

    /**
     * Constructs a dictionary remove fields error with a cause.
     *
     * @param cause the cause.
     */
    public Error(SdkException cause) {
      super(cause);
    }
  }
}
