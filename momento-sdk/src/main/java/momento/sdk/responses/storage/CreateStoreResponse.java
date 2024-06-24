package momento.sdk.responses.storage;

import momento.sdk.exceptions.SdkException;
import momento.sdk.internal.StringHelpers;

/** Response for a create store operation */
public interface CreateStoreResponse {
  /** A successful create store operation. */
  class Success implements CreateStoreResponse {
    @Override
    public String toString() {
      return StringHelpers.emptyToString("CreateStoreResponse.Success");
    }
  }

  /** Indicates that the store already exists, so there was no need to create it. */
  class AlreadyExists implements CreateStoreResponse {
    @Override
    public String toString() {
      return StringHelpers.emptyToString("CreateStoreResponse.AlreadyExists");
    }
  }

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

    @Override
    public String toString() {
      return toStringTemplate("CreateStoreResponse.Error");
    }
  }
}
