package momento.sdk.messages;

import grpc.cache_client._SortedSetElement;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import momento.sdk.exceptions.SdkException;

/** Response for a sorted set fetch operation */
public interface CacheSortedSetFetchResponse {

  /** A successful sorted set fetch operation that found elements. */
  class Hit implements CacheSortedSetFetchResponse {
    private final List<_SortedSetElement> elements;

    /**
     * Constructs a sorted set fetch hit with a list of encoded elements.
     *
     * @param elements the retrieved elements.
     */
    public Hit(List<_SortedSetElement> elements) {
      this.elements = elements;
    }

    /**
     * Gets a navigable set of the retrieved elements and their scores. The set is in ascending
     * order by default and can be viewed in descending order with {@link
     * NavigableSet#descendingSet()}.
     *
     * @return An ordered set of elements to scores
     */
    public NavigableSet<ScoredElement> elementsSet() {
      return elements.stream()
          .map(e -> new ScoredElement(e.getValue(), e.getScore()))
          .collect(Collectors.toCollection(TreeSet::new));
    }
  }

  /** A successful sorted set fetch operation that did not find elements. */
  class Miss implements CacheSortedSetFetchResponse {}

  /**
   * A failed sorted set fetch operation. The response itself is an exception, so it can be directly
   * thrown, or the cause of the error can be retrieved with {@link #getCause()}. The message is a
   * copy of the message of the cause.
   */
  class Error extends SdkException implements CacheSortedSetFetchResponse {

    /**
     * Constructs a sorted set fetch error with a cause.
     *
     * @param cause the cause.
     */
    public Error(SdkException cause) {
      super(cause);
    }
  }
}
