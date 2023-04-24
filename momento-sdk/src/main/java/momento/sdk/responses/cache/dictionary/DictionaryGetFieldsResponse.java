package momento.sdk.responses.cache.dictionary;

import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import momento.sdk.exceptions.SdkException;
import momento.sdk.internal.StringHelpers;

/** Response for a dictionary get fields operation */
public interface DictionaryGetFieldsResponse {
  /**
   * A successful dictionary get fields operation that found the fields with values in the
   * dictionary.
   */
  class Hit implements DictionaryGetFieldsResponse {
    private final List<DictionaryGetFieldResponse> responses;

    /**
     * Constructs a dictionary get fields hit with a list of encoded getField responses.
     *
     * @param responses The retrieved responses.
     */
    public Hit(List<DictionaryGetFieldResponse> responses) {
      this.responses = responses;
    }

    /**
     * Gets a {@link DictionaryGetFieldResponse} for each looked up field.
     *
     * @return The responses.
     */
    public List<DictionaryGetFieldResponse> perFieldResponses() {
      return responses;
    }

    /**
     * Gets the retrieved map of UTF-8 string fields to UTF-8 string values.
     *
     * @return The map.
     */
    public Map<String, String> valueMapStringString() {
      return responses.stream()
          .filter(r -> r instanceof DictionaryGetFieldResponse.Hit)
          .collect(
              Collectors.toMap(
                  r -> ((DictionaryGetFieldResponse.Hit) r).fieldString(),
                  r -> ((DictionaryGetFieldResponse.Hit) r).valueString()));
    }

    /**
     * Gets the retrieved map of UTF-8 string fields to UTF-8 string values.
     *
     * @return The map.
     */
    public Map<String, String> valueMap() {
      return valueMapStringString();
    }

    /**
     * Gets the retrieved map of UTF-8 string fields to byte array values.
     *
     * @return The map.
     */
    public Map<String, byte[]> valueMapStringByteArray() {
      return responses.stream()
          .filter(r -> r instanceof DictionaryGetFieldResponse.Hit)
          .collect(
              Collectors.toMap(
                  r -> ((DictionaryGetFieldResponse.Hit) r).fieldString(),
                  r -> ((DictionaryGetFieldResponse.Hit) r).valueByteArray()));
    }

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

  /**
   * A successful dictionary get fields operation for a key that does not exist in the dictionary.
   */
  class Miss implements DictionaryGetFieldsResponse {}

  /**
   * A failed dictionary get fields operation. The response itself is an exception, so it can be
   * directly thrown, or the cause of the error can be retrieved with {@link #getCause()}. The
   * message is a copy of the message of the cause.
   */
  class Error extends SdkException implements DictionaryGetFieldsResponse {

    /**
     * Constructs a dictionary get fields error with a cause.
     *
     * @param cause The cause.
     */
    public Error(SdkException cause) {
      super(cause);
    }
  }
}
