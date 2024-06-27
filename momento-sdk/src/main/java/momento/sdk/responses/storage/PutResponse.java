package momento.sdk.responses.storage;

import momento.sdk.exceptions.SdkException;
import momento.sdk.internal.StringHelpers;

/** Response for a set operation */
public interface PutResponse {

  /** A successful set operation. */
  class Success implements PutResponse {
    @Override
    public String toString() {
      return StringHelpers.emptyToString("PutResponse.Success");
    }
  }

  /**
   * A failed set operation. The response itself is an exception, so it can be directly thrown, or
   * the cause of the error can be retrieved with {@link #getCause()}. The message is a copy of the
   * message of the cause.
   */
  class Error extends SdkException implements PutResponse {

    /**
     * Constructs a persistent store set error with a cause.
     *
     * @param cause the cause.
     */
    public Error(SdkException cause) {
      super(cause);
    }

    @Override
    public String toString() {
      return buildToString("PutResponse.Error");
    }
  }
}
