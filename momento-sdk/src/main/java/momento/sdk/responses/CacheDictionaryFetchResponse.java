package momento.sdk.responses;

import com.google.protobuf.ByteString;
import java.util.Base64;
import java.util.Map;
import java.util.stream.Collectors;
import momento.sdk.exceptions.SdkException;
import momento.sdk.internal.StringHelpers;

/** Response for a dictionary fetch operation */
public interface CacheDictionaryFetchResponse {

  /** A successful dictionary fetch operation that found elements. */
  class Hit implements CacheDictionaryFetchResponse {
    private final Map<ByteString, ByteString> fieldsToValues;

    /**
     * Constructs a dictionary fetch hit with a map of encoded fields to values.
     *
     * @param fieldsToValues The retrieved map.
     */
    public Hit(Map<ByteString, ByteString> fieldsToValues) {
      this.fieldsToValues = fieldsToValues;
    }

    /**
     * Gets the retrieved elements as a map of UTF-8 string fields to UTF-8 string values.
     *
     * @return The map.
     */
    public Map<String, String> valueMapStringString() {
      return fieldsToValues.entrySet().stream()
          .collect(
              Collectors.toMap(
                  entry -> entry.getKey().toStringUtf8(),
                  entry -> entry.getValue().toStringUtf8()));
    }

    /**
     * Gets the retrieved elements as a map of UTF-8 string fields to UTF-8 string values.
     *
     * @return The map.
     */
    public Map<String, String> valueMap() {
      return valueMapStringString();
    }

    /**
     * Gets the retrieved elements as a map of UTF-8 string fields to byte array values.
     *
     * @return The map.
     */
    public Map<String, byte[]> valueMapStringByteArray() {
      return fieldsToValues.entrySet().stream()
          .collect(
              Collectors.toMap(
                  entry -> entry.getKey().toStringUtf8(), entry -> entry.getValue().toByteArray()));
    }

    /**
     * {@inheritDoc}
     *
     * <p>Truncates the internal fields to 20 characters to bound the size of the string.
     */
    @Override
    public String toString() {
      final String stringStringRepresentation =
          valueMapStringString().entrySet().stream()
              .map(e -> e.getKey() + ":" + e.getValue())
              .limit(5)
              .map(StringHelpers::truncate)
              .collect(Collectors.joining(", ", "\"", "\"..."));

      final String stringBytesRepresentation =
          valueMapStringByteArray().entrySet().stream()
              .map(e -> e.getKey() + ":" + Base64.getEncoder().encodeToString(e.getValue()))
              .limit(5)
              .map(StringHelpers::truncate)
              .collect(Collectors.joining(", ", "\"", "\"..."));

      return super.toString()
          + ": valueMapStringString: "
          + stringStringRepresentation
          + " valueMapStringByteArray: "
          + stringBytesRepresentation;
    }
  }

  /** A successful dictionary fetch operation that did not find dictionary. */
  class Miss implements CacheDictionaryFetchResponse {}

  /**
   * A failed dictionary fetch operation. The response itself is an exception, so it can be directly
   * thrown, or the cause of the error can be retrieved with {@link #getCause()}. The message is a
   * copy of the message of the cause.
   */
  class Error extends SdkException implements CacheDictionaryFetchResponse {

    /**
     * Constructs a dictionary fetch error with a cause.
     *
     * @param cause The cause.
     */
    public Error(SdkException cause) {
      super(cause);
    }
  }
}
