package momento.sdk.messages;

import momento.sdk.exceptions.SdkException;

/** Response for a sorted set increment score operation */
public interface CacheSortedSetIncrementScoreResponse {

  /** A successful sorted set increment score operation. */
  class Success implements CacheSortedSetIncrementScoreResponse {

    private final double score;

    /**
     * Constructs a successful sorted set increment score response with the new score.
     *
     * @param score the new score.
     */
    public Success(double score) {
      this.score = score;
    }

    /**
     * Gets the newly updated score.
     *
     * @return the score.
     */
    public double score() {
      return score;
    }

    @Override
    public String toString() {
      return super.toString() + ": score: \"" + score + "\"";
    }
  }

  /**
   * A failed sorted set increment score operation. The response itself is an exception, so it can
   * be directly thrown, or the cause of the error can be retrieved with {@link #getCause()}. The
   * message is a copy of the message of the cause.
   */
  class Error extends SdkException implements CacheSortedSetIncrementScoreResponse {

    /**
     * Constructs a sorted set increment score error with a cause.
     *
     * @param cause the cause.
     */
    public Error(SdkException cause) {
      super(cause);
    }
  }
}
