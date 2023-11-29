package momento.sdk.batchutils.request;

import java.util.Collection;

/**
 * Represents a batch get request with a collection of keys.
 *
 * @param <T> The type of the keys in the batch request.
 */
public class BatchGetRequest<T> {
  private final Collection<T> keys;

  /**
   * Constructs a BatchGetRequest with the specified collection of keys.
   *
   * @param keys The collection of keys for the batch get request.
   */
  public BatchGetRequest(Collection<T> keys) {
    this.keys = keys;
  }

  /**
   * Returns the collection of keys in this batch get request.
   *
   * @return The collection of keys.
   */
  public Collection<T> getKeys() {
    return keys;
  }

  /** Specialized version of BatchGetRequest for String keys. */
  public static class StringKeyBatchGetRequest extends BatchGetRequest<String> {
    /**
     * Constructs a StringKeyBatchGetRequest with the specified collection of String keys.
     *
     * @param keys The collection of String keys for the batch get request.
     */
    public StringKeyBatchGetRequest(Collection<String> keys) {
      super(keys);
    }
  }

  /** Specialized version of BatchGetRequest for byte array keys. */
  public static class ByteArrayKeyBatchGetRequest extends BatchGetRequest<byte[]> {
    /**
     * Constructs a ByteArrayKeyBatchGetRequest with the specified collection of byte array keys.
     *
     * @param keys The collection of byte array keys for the batch get request.
     */
    public ByteArrayKeyBatchGetRequest(Collection<byte[]> keys) {
      super(keys);
    }
  }
}
