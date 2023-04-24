package momento.sdk.responses.cache.dictionary;

import momento.sdk.exceptions.SdkException;

/** Response for a dictionary remove fields operation */
public interface DictionaryRemoveFieldsResponse {
  /** A successful dictionary remove fields operation. */
  class Success implements DictionaryRemoveFieldsResponse {}

  /**
   * A failed dictionary remove fields operation. The response itself is an exception, so it can be
   * directly thrown, or the cause of the error can be retrieved with {@link #getCause()}. The
   * message is a copy of the message of the cause.
   */
  class Error extends SdkException implements DictionaryRemoveFieldsResponse {

    /**
     * Constructs a dictionary remove fields error with a cause.
     *
     * @param cause The cause.
     */
    public Error(SdkException cause) {
      super(cause);
    }
  }
}
