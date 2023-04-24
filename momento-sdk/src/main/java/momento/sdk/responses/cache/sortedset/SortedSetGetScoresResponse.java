package momento.sdk.responses.cache.sortedset;

import java.util.List;
import java.util.stream.Collectors;
import momento.sdk.exceptions.SdkException;

/** Response for a sorted set get scores operation */
public interface SortedSetGetScoresResponse {

  /** A successful sorted set get scores operation where an element was found. */
  class Hit implements SortedSetGetScoresResponse {
    private final List<SortedSetGetScoreResponse> responses;

    /**
     * Constructs a sorted set get scores response with scores.
     *
     * @param responses the elements to their retrieved scores.
     */
    public Hit(List<SortedSetGetScoreResponse> responses) {
      this.responses = responses;
    }

    /**
     * Gets a map of string elements with scores to those scores.
     *
     * @return the map.
     */
    public List<ScoredElement> scoredElements() {
      return this.responses.stream()
          .filter(e -> e instanceof SortedSetGetScoreResponse.Hit)
          .map(
              e ->
                  new ScoredElement(
                      ((SortedSetGetScoreResponse.Hit) e).element,
                      ((SortedSetGetScoreResponse.Hit) e).score))
          .collect(Collectors.toList());
    }

    @Override
    public String toString() {
      return this.getClass().getSimpleName()
          + ": "
          + responses.stream()
              .limit(5)
              .map(Object::toString)
              .collect(Collectors.joining(", ", "", "..."));
    }
  }

  /** A successful sorted set get scores operation where no scores were found. */
  class Miss implements SortedSetGetScoresResponse {}

  /**
   * A failed sorted set get scores operation. The response itself is an exception, so it can be
   * directly thrown, or the cause of the error can be retrieved with {@link #getCause()}. The
   * message is a copy of the message of the cause.
   */
  class Error extends SdkException implements SortedSetGetScoresResponse {

    /**
     * Constructs a sorted set get scores error with a cause.
     *
     * @param cause the cause.
     */
    public Error(SdkException cause) {
      super(cause);
    }
  }
}
