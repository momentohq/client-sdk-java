package momento.sdk.responses;

import grpc.cache_client._SortedSetElement;
import java.util.Base64;
import java.util.List;
import java.util.stream.Collectors;
import momento.sdk.exceptions.SdkException;
import momento.sdk.internal.StringHelpers;

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
     * Gets a list of the retrieved elements and their scores. The set is ordered by the {@link
     * SortOrder} used in the fetch method call or ascending if no order was specified.
     *
     * @return An ordered list of elements and their scores
     */
    public List<ScoredElement> elementsList() {
      return elements.stream()
          .map(e -> new ScoredElement(e.getValue(), e.getScore()))
          .collect(Collectors.toList());
    }

    /**
     * {@inheritDoc}
     *
     * <p>Truncates the internal fields to 20 characters to bound the size of the string.
     */
    @Override
    public String toString() {
      return elements.stream()
          .limit(5)
          .map(
              e ->
                  "valueString: "
                      + StringHelpers.truncate(e.getValue().toStringUtf8())
                      + " valueBytes: "
                      + StringHelpers.truncate(
                          Base64.getEncoder().encodeToString(e.getValue().toByteArray()))
                      + " score: "
                      + e.getScore())
          .collect(Collectors.joining(", ", "", "..."));
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
