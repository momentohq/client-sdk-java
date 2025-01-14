package momento.sdk.responses.leaderboard;

import momento.sdk.exceptions.SdkException;
import momento.sdk.internal.StringHelpers;

/** Response for a leaderboard length operation */
public interface LengthResponse {

  /** A successful length operation. */
  class Success implements LengthResponse {
    private final int length;

    public Success(int length) {
      this.length = length;
    }

    /**
     * Gets the length of the leaderboard.
     *
     * @return the length.
     */
    public int length() {
      return length;
    }

    @Override
    public String toString() {
      return StringHelpers.emptyToString("LengthResponse.Success") + ": length: " + length;
    }
  }

  /**
   * A failed length operation. The response itself is an exception, so it can be directly thrown,
   * or the cause of the error can be retrieved with {@link #getCause()}. The message is a copy of
   * the message of the cause.
   */
  class Error extends SdkException implements LengthResponse {

    /**
     * Constructs a leaderboard length error with a cause.
     *
     * @param cause the cause.
     */
    public Error(SdkException cause) {
      super(cause);
    }

    @Override
    public String toString() {
      return buildToString("LengthResponse.Error");
    }
  }
}
