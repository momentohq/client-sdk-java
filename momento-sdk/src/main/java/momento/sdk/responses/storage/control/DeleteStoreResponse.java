package momento.sdk.responses.storage.control;

import momento.sdk.exceptions.SdkException;

/** Response for a delete store operation */
public interface DeleteStoreResponse {

  /** A successful delete store operation. */
  class Success implements DeleteStoreResponse {}

  /**
   * A failed delete store operation. The response itself is an exception, so it can be directly
   * thrown, or the cause of the error can be retrieved with {@link #getCause()}. The message is a
   * copy of the message of the cause.
   */
  class Error extends SdkException implements DeleteStoreResponse {

    /**
     * Constructs a delete store error with a cause.
     *
     * @param cause the cause.
     */
    public Error(SdkException cause) {
      super(cause);
    }
  }
}
