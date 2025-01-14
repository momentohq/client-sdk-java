package momento.sdk.responses.leaderboard;

import momento.sdk.exceptions.SdkException;
import momento.sdk.internal.StringHelpers;

/** Response for a leaderboard delete operation */
public interface DeleteResponse {

  /** A successful delete operation. */
  class Success implements DeleteResponse {
    @Override
    public String toString() {
      return StringHelpers.emptyToString("DeleteResponse.Success");
    }
  }

  /**
   * A failed delete operation. The response itself is an exception, so it can be directly thrown,
   * or the cause of the error can be retrieved with {@link #getCause()}. The message is a copy of
   * the message of the cause.
   */
  class Error extends SdkException implements DeleteResponse {

    /**
     * Constructs a leaderboard delete error with a cause.
     *
     * @param cause the cause.
     */
    public Error(SdkException cause) {
      super(cause);
    }

    @Override
    public String toString() {
      return buildToString("DeleteResponse.Error");
    }
  }
}
