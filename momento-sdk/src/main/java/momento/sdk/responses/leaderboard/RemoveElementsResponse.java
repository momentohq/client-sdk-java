package momento.sdk.responses.leaderboard;

import momento.sdk.exceptions.SdkException;
import momento.sdk.internal.StringHelpers;

/** Response for a leaderboard remove elements operation */
public interface RemoveElementsResponse {

  /** A successful remove elements operation. */
  class Success implements RemoveElementsResponse {
    @Override
    public String toString() {
      return StringHelpers.emptyToString("RemoveElementsResponse.Success");
    }
  }

  /**
   * A failed remove operation. The response itself is an exception, so it can be directly thrown,
   * or the cause of the error can be retrieved with {@link #getCause()}. The message is a copy of
   * the message of the cause.
   */
  class Error extends SdkException implements RemoveElementsResponse {

    /**
     * Constructs a leaderboard remove elements error with a cause.
     *
     * @param cause the cause.
     */
    public Error(SdkException cause) {
      super(cause);
    }

    @Override
    public String toString() {
      return buildToString("RemoveElementsResponse.Error");
    }
  }
}
