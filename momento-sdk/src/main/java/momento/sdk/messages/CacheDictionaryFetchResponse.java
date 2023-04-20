package momento.sdk.messages;

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
     * Constructs a dictionary fetch hit with a list of encoded keys and values.
     *
     * @param fieldsToValues the retrieved dictionary.
     */
    public Hit(Map<ByteString, ByteString> fieldsToValues) {
      this.fieldsToValues = fieldsToValues;
    }

    /**
     * Gets the retrieved values as a dictionary of UTF-8 string keys and values.
     *
     * @return the dictionary.
     */
    public Map<String, String> valueDictionaryStringString() {
      return fieldsToValues.entrySet().stream()
          .collect(
              Collectors.toMap(
                  entry -> entry.getKey().toStringUtf8(),
                  entry -> entry.getValue().toStringUtf8()));
    }

    /**
     * Gets the retrieved values as a dictionary of UTF-8 string keys and values.
     *
     * @return the dictionary.
     */
    public Map<String, String> valueDictionary() {
      return valueDictionaryStringString();
    }

    /**
     * Gets the retrieved value as a dictionary of UTF-8 String keys and byte array values
     *
     * @return the dictionary.
     */
    public Map<String, byte[]> valueDictionaryStringBytes() {
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
          valueDictionaryStringString().entrySet().stream()
              .map(e -> e.getKey() + ":" + e.getValue())
              .limit(5)
              .map(StringHelpers::truncate)
              .collect(Collectors.joining(", ", "\"", "\"..."));

      final String stringBytesRepresentation =
          valueDictionaryStringBytes().entrySet().stream()
              .map(e -> e.getKey() + ":" + Base64.getEncoder().encodeToString(e.getValue()))
              .limit(5)
              .map(StringHelpers::truncate)
              .collect(Collectors.joining(", ", "\"", "\"..."));

      return super.toString()
          + ": valueStringString: "
          + stringStringRepresentation
          + " valueStringBytes: "
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
     * @param cause the cause.
     */
    public Error(SdkException cause) {
      super(cause);
    }
  }
}
