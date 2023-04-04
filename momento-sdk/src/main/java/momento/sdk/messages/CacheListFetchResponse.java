package momento.sdk.messages;

import com.google.protobuf.ByteString;
import java.util.Base64;
import java.util.List;
import java.util.stream.Collectors;
import momento.sdk.exceptions.SdkException;
import momento.sdk.internal.StringHelpers;

/** Response for a list fetch operation */
public interface CacheListFetchResponse {

  /** A successful list fetch operation that found elements. */
  class Hit implements CacheListFetchResponse {
    private List<ByteString> byteStringValues;

    /**
     * Constructs a list fetch hit with a list of encoded values.
     *
     * @param values the retrieved values.
     */
    public Hit(List<ByteString> values) {
      this.byteStringValues = values;
    }

    /**
     * Gets the retrieved values as a list of byte arrays.
     *
     * @return the values.
     */
    public List<byte[]> valueListByteArray() {
      return byteStringValues.stream().map(ByteString::toByteArray).collect(Collectors.toList());
    }

    /**
     * Gets the retrieved value as a list of UTF-8 Strings
     *
     * @return the values.
     */
    public List<String> valueListString() {
      return byteStringValues.stream().map(ByteString::toStringUtf8).collect(Collectors.toList());
    }

    /**
     * {@inheritDoc}
     *
     * <p>Truncates the internal fields to 20 characters to bound the size of the string.
     */
    @Override
    public String toString() {
      final String stringRepresentation =
          valueListString().stream()
              .limit(5)
              .map(StringHelpers::truncate)
              .collect(Collectors.joining(", ", "\"", "\"..."));

      final String bytesRepresentation =
          valueListByteArray().stream()
              .limit(5)
              .map(ba -> Base64.getEncoder().encodeToString(ba))
              .map(StringHelpers::truncate)
              .collect(Collectors.joining(", ", "\"", "\"..."));

      return super.toString()
          + ": valueSetString: "
          + stringRepresentation
          + " valueSetByteArray: "
          + bytesRepresentation;
    }
  }

  /** A successful list fetch operation that did not find elements. */
  class Miss implements CacheListFetchResponse {}

  /**
   * A failed list fetch operation. The response itself is an exception, so it can be directly
   * thrown, or the cause of the error can be retrieved with {@link #getCause()}. The message is a
   * copy of the message of the cause.
   */
  class Error extends SdkException implements CacheListFetchResponse {

    /**
     * Constructs a list fetch error with a cause.
     *
     * @param cause the cause.
     */
    public Error(SdkException cause) {
      super(cause);
    }
  }
}
