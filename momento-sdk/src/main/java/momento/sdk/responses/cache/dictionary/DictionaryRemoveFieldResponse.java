package momento.sdk.responses.cache.dictionary;

import momento.sdk.exceptions.SdkException;

/** Response for a dictionary remove field operation */
public interface DictionaryRemoveFieldResponse {
  /** A successful dictionary remove field operation. */
  class Success implements DictionaryRemoveFieldResponse {}

  /**
   * A failed dictionary remove field operation. The response itself is an exception, so it can be
   * directly thrown, or the cause of the error can be retrieved with {@link #getCause()}. The
   * message is a copy of the message of the cause.
   */
  class Error extends SdkException implements DictionaryRemoveFieldResponse {

    /**
     * Constructs a dictionary remove field error with a cause.
     *
     * @param cause The cause.
     */
    public Error(SdkException cause) {
      super(cause);
    }
  }
}
