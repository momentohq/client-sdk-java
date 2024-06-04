package momento.sdk.responses.storage.control;

import momento.sdk.exceptions.SdkException;

/** Response for a create persistent store operation */
public interface CreatePersistentStoreResponse {
  /** A successful create persistent store operation. */
  class Success implements CreatePersistentStoreResponse {}

  /**
   * A failed create persistent store operation. The response itself is an exception, so it can be
   * directly thrown, or the cause of the error can be retrieved with {@link #getCause()}. The
   * message is a copy of the message of the cause.
   */
  class Error extends SdkException implements CreatePersistentStoreResponse {

    /**
     * Constructs a cache creation error with a cause.
     *
     * @param cause the cause.
     */
    public Error(SdkException cause) {
      super(cause);
    }
  }
}
