package momento.sdk.responses.storage;

import momento.sdk.exceptions.SdkException;
import momento.sdk.internal.StringHelpers;

/** Response for a delete store operation */
public interface DeleteStoreResponse {

  /** A successful delete store operation. */
  class Success implements DeleteStoreResponse {
    @Override
    public String toString() {
      return StringHelpers.emptyToString("DeleteStoreResponse.Success");
    }
  }

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

    @Override
    public String toString() {
      return toStringTemplate("DeleteStoreResponse.Error");
    }
  }
}
