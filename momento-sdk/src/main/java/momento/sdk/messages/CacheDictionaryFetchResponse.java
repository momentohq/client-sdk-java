package momento.sdk.messages;

import com.google.protobuf.ByteString;
import grpc.cache_client._DictionaryFieldValuePair;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import momento.sdk.exceptions.SdkException;
import momento.sdk.internal.StringHelpers;

/** Response for a dictionary fetch operation */
public interface CacheDictionaryFetchResponse {

  /** A successful dictionary fetch operation that found elements. */
  class Hit implements CacheDictionaryFetchResponse {
    private final Map<ByteString, ByteString> byteStringKeysValues;

    /**
     * Constructs a dictionary fetch hit with a list of encoded keys and values.
     *
     * @param byteStringKeysValues the retrieved dictionary.
     */
    public Hit(List<_DictionaryFieldValuePair> byteStringKeysValues) {
      this.byteStringKeysValues =
          byteStringKeysValues.stream()
              .collect(
                  Collectors.toMap(
                      _DictionaryFieldValuePair::getField, _DictionaryFieldValuePair::getValue));
    }

    /**
     * Gets the retrieved values as a dictionary of byte array keys and values.
     *
     * @return the dictionary.
     */
    public Map<byte[], byte[]> valueDictionaryBytesBytes() {
      return byteStringKeysValues.entrySet().stream()
          .collect(
              Collectors.toMap(
                  entry -> entry.getKey().toByteArray(), entry -> entry.getValue().toByteArray()));
    }

    /**
     * Gets the retrieved values as a dictionary of UTF-8 string keys and values.
     *
     * @return the dictionary.
     */
    public Map<String, String> valueDictionaryStringString() {
      return byteStringKeysValues.entrySet().stream()
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
      return byteStringKeysValues.entrySet().stream()
          .collect(
              Collectors.toMap(
                  entry -> entry.getKey().toStringUtf8(),
                  entry -> entry.getValue().toStringUtf8()));
    }

    /**
     * Gets the retrieved value as a dictionary of UTF-8 String keys and byte array values
     *
     * @return the dictionary.
     */
    public Map<String, byte[]> valueDictionaryStringBytes() {
      return byteStringKeysValues.entrySet().stream()
          .collect(
              Collectors.toMap(
                  entry -> entry.getKey().toStringUtf8(), entry -> entry.getValue().toByteArray()));
    }

    /**
     * Gets the retrieved value as a dictionary of byte array keys and UTF-8 String values
     *
     * @return the dictionary.
     */
    public Map<byte[], String> valueDictionaryBytesString() {
      return byteStringKeysValues.entrySet().stream()
          .collect(
              Collectors.toMap(
                  entry -> entry.getKey().toByteArray(), entry -> entry.getValue().toStringUtf8()));
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

      final String bytesBytesRepresentation =
          valueDictionaryBytesBytes().entrySet().stream()
              .map(
                  e ->
                      Base64.getEncoder().encodeToString(e.getKey())
                          + ":"
                          + Base64.getEncoder().encodeToString(e.getValue()))
              .limit(5)
              .map(StringHelpers::truncate)
              .collect(Collectors.joining(", ", "\"", "\"..."));

      final String stringBytesRepresentation =
          valueDictionaryStringBytes().entrySet().stream()
              .map(e -> e.getKey() + ":" + Base64.getEncoder().encodeToString(e.getValue()))
              .limit(5)
              .map(StringHelpers::truncate)
              .collect(Collectors.joining(", ", "\"", "\"..."));

      final String bytesStringRepresentation =
          valueDictionaryBytesString().entrySet().stream()
              .map(e -> Base64.getEncoder().encodeToString(e.getKey()) + ":" + e.getValue())
              .limit(5)
              .map(StringHelpers::truncate)
              .collect(Collectors.joining(", ", "\"", "\"..."));

      return super.toString()
          + ": valueStringString: "
          + stringStringRepresentation
          + " valueByteBytes: "
          + bytesBytesRepresentation
          + " valueStringBytes: "
          + stringBytesRepresentation
          + " valueBytesString: "
          + bytesStringRepresentation;
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
