package momento.sdk.responses.cache.set;

import com.google.protobuf.ByteString;
import java.util.Base64;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import momento.sdk.exceptions.SdkException;
import momento.sdk.internal.StringHelpers;

/** Response for a set fetch operation */
public interface SetFetchResponse {

  /** A successful set fetch operation that found elements. */
  class Hit implements SetFetchResponse {
    private final List<ByteString> byteStringValues;

    /**
     * Constructs a set fetch hit with a list of encoded values.
     *
     * @param values the retrieved values.
     */
    public Hit(List<ByteString> values) {
      this.byteStringValues = values;
    }

    /**
     * Gets the retrieved values as a set of byte arrays.
     *
     * @return the values.
     */
    public Set<byte[]> valueSetByteArray() {
      return byteStringValues.stream().map(ByteString::toByteArray).collect(Collectors.toSet());
    }

    /**
     * Gets the retrieved value as a set of UTF-8 Strings
     *
     * @return the values.
     */
    public Set<String> valueSetString() {
      return byteStringValues.stream().map(ByteString::toStringUtf8).collect(Collectors.toSet());
    }

    /**
     * Gets the retrieved value as a set of UTF-8 Strings
     *
     * @return the values.
     */
    public Set<String> valueSet() {
      return valueSetString();
    }

    /**
     * {@inheritDoc}
     *
     * <p>Truncates the internal fields to 20 characters to bound the size of the string.
     */
    @Override
    public String toString() {
      final String stringRepresentation =
          valueSetString().stream()
              .limit(5)
              .map(StringHelpers::truncate)
              .collect(Collectors.joining(", ", "\"", "\"..."));

      final String bytesRepresentation =
          valueSetByteArray().stream()
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

  /** A successful set fetch operation that did not find elements. */
  class Miss implements SetFetchResponse {}

  /**
   * A failed set fetch operation. The response itself is an exception, so it can be directly
   * thrown, or the cause of the error can be retrieved with {@link #getCause()}. The message is a
   * copy of the message of the cause.
   */
  class Error extends SdkException implements SetFetchResponse {

    /**
     * Constructs a set fetch error with a cause.
     *
     * @param cause the cause.
     */
    public Error(SdkException cause) {
      super(cause);
    }
  }
}
