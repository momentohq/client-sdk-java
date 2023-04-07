package momento.sdk.messages;

import grpc.cache_client._SortedSetElement;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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

    public NavigableSet<ScoredElement<String>> elementsSet() {
      return elements.stream()
          .map(e -> new ScoredElement<>(e.getValue().toStringUtf8(), e.getScore()))
          .collect(Collectors.toCollection(TreeSet::new));
    }

    public List<ScoredElement<String>> elementsList() {
      return elements.stream()
          .map(e -> new ScoredElement<>(e.getValue().toStringUtf8(), e.getScore()))
          .collect(Collectors.toList());
    }

    public List<ScoredElement<byte[]>> elementsByteArrayList() {
      return elements.stream()
          .map(e -> new ScoredElement<>(e.getValue().toByteArray(), e.getScore()))
          .collect(Collectors.toList());
    }

    /**
     * Gets a map of the retrieved elements as UTF-8 strings to their scores. The order of the map
     * is determined by the options used in the method call.
     *
     * @return An ordered map of String elements to scores
     */
    public Map<String, Double> elementsMap() {
      return elements.stream()
          .collect(
              Collectors.toMap(
                  e -> e.getValue().toStringUtf8(),
                  _SortedSetElement::getScore,
                  (u, v) -> u,
                  LinkedHashMap::new));
    }

    /**
     * Gets a map of the retrieved elements as byte arrays to their scores. The order of the map is
     * determined by the options used in the method call.
     *
     * @return An ordered map of byte[] elements to scores
     */
    public Map<byte[], Double> elementsByteArrayMap() {
      return elements.stream()
          .collect(
              Collectors.toMap(
                  e -> e.getValue().toByteArray(),
                  _SortedSetElement::getScore,
                  (u, v) -> u,
                  LinkedHashMap::new));
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
