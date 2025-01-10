package momento.sdk.responses.leaderboard;

import java.util.List;
import java.util.stream.Collectors;
import momento.sdk.exceptions.SdkException;
import momento.sdk.responses.SortOrder;

/** Response for a leaderboard fetch operation */
public interface FetchResponse {

  /** A successful leaderboard fetch operation. */
  class Success implements FetchResponse {
    private final List<LeaderboardElement> elements;

    /**
     * Constructs a leaderboard fetch success with a list of elements.
     *
     * @param elements the retrieved elements.
     */
    public Success(List<LeaderboardElement> elements) {
      this.elements = elements;
    }

    /**
     * Returns a list of the retrieved IDs and their scores. The set is ordered by the {@link
     * SortOrder} used in the fetch method call or ascending if no order was specified.
     *
     * @return An ordered list of IDs and their scores
     */
    public List<LeaderboardElement> elementsList() {
      return elements;
    }

    /**
     * Returns a list of the retrieved IDs and their scores. The set is ordered by the {@link
     * SortOrder} used in the fetch method call or ascending if no order was specified.
     *
     * @return An ordered list of IDs and their scores
     */
    public List<LeaderboardElement> values() {
      return elements;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Truncates the list of elements to bound the size of the string.
     */
    @Override
    public String toString() {
      return elements.stream()
          .limit(5)
          .map(e -> "ID: " + e.getId() + " score: " + e.getScore())
          .collect(Collectors.joining(", ", "", "..."));
    }
  }

  /**
   * A failed leaderboard fetch operation. The response itself is an exception, so it can be
   * directly thrown, or the cause of the error can be retrieved with {@link #getCause()}. The
   * message is a copy of the message of the cause.
   */
  class Error extends SdkException implements FetchResponse {

    /**
     * Constructs a leaderboard fetch error with a cause.
     *
     * @param cause the cause.
     */
    public Error(SdkException cause) {
      super(cause);
    }
  }
}
