package momento.sdk.responses.persistent_store.control;

import momento.sdk.exceptions.SdkException;

/** Response for a delete persistent store operation */
public interface DeletePersistentStoreResponse {

  /** A successful delete persistent store operation. */
  class Success implements DeletePersistentStoreResponse {}

  /**
   * A failed delete persistent store operation. The response itself is an exception, so it can be
   * directly thrown, or the cause of the error can be retrieved with {@link #getCause()}. The
   * message is a copy of the message of the cause.
   */
  class Error extends SdkException implements DeletePersistentStoreResponse {

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
