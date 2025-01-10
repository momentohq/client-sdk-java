package momento.sdk.responses.leaderboard;

import momento.sdk.exceptions.SdkException;
import momento.sdk.internal.StringHelpers;

/** Response for a leaderboard upsert operation */
public interface UpsertResponse {

  /** A successful upsert operation. */
  class Success implements UpsertResponse {
    @Override
    public String toString() {
      return StringHelpers.emptyToString("UpsertResponse.Success");
    }
  }

  /**
   * A failed upsert operation. The response itself is an exception, so it can be directly thrown,
   * or the cause of the error can be retrieved with {@link #getCause()}. The message is a copy of
   * the message of the cause.
   */
  class Error extends SdkException implements UpsertResponse {

    /**
     * Constructs a leaderboard upsert error with a cause.
     *
     * @param cause the cause.
     */
    public Error(SdkException cause) {
      super(cause);
    }

    @Override
    public String toString() {
      return buildToString("UpsertResponse.Error");
    }
  }
}
