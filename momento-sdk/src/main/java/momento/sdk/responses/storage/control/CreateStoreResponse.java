package momento.sdk.responses.storage.control;

import momento.sdk.exceptions.SdkException;

/** Response for a create store operation */
public interface CreateStoreResponse {
  /** A successful create store operation. */
  class Success implements CreateStoreResponse {}

  /** Indicates that the store already exists, so there was no need to create it. */
  class AlreadyExists implements CreateStoreResponse {}

  /**
   * A failed create store operation. The response itself is an exception, so it can be directly
   * thrown, or the cause of the error can be retrieved with {@link #getCause()}. The message is a
   * copy of the message of the cause.
   */
  class Error extends SdkException implements CreateStoreResponse {

    /**
     * Constructs a create store error with a cause.
     *
     * @param cause the cause.
     */
    public Error(SdkException cause) {
      super(cause);
    }
  }
}
