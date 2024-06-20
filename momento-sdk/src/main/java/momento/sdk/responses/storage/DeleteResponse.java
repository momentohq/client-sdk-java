package momento.sdk.responses.storage;

import momento.sdk.exceptions.SdkException;

/** Response for a delete operation */
public interface DeleteResponse {

  /** A successful delete operation. */
  class Success implements DeleteResponse {}

  /**
   * A failed delete operation. The response itself is an exception, so it can be directly thrown,
   * or the cause of the error can be retrieved with {@link #getCause()}. The message is a copy of
   * the message of the cause.
   */
  class Error extends SdkException implements DeleteResponse {

    /**
     * Constructs a persistent store delete error with a cause.
     *
     * @param cause the cause.
     */
    public Error(SdkException cause) {
      super(cause);
    }
  }
}
