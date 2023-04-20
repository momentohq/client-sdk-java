package momento.sdk.messages;

import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import momento.sdk.exceptions.SdkException;
import momento.sdk.internal.StringHelpers;

/** Response for a dictionary get fields operation */
public interface CacheDictionaryGetFieldsResponse {
  /**
   * A successful dictionary get fields operation that found the keys with values in the dictionary.
   */
  class Hit implements CacheDictionaryGetFieldsResponse {
    private final List<CacheDictionaryGetFieldResponse> responses;

    /**
     * Constructs a dictionary get fields hit with a list of encoded keys and values.
     *
     * @param responses the retrieved dictionary.
     */
    public Hit(List<CacheDictionaryGetFieldResponse> responses) {
      this.responses = responses;
    }

    /**
     * Gets a {@link CacheDictionaryGetFieldResponse} for each looked up value.
     *
     * @return the responses.
     */
    public List<CacheDictionaryGetFieldResponse> perFieldResponses() {
      return responses;
    }

    /**
     * Gets the retrieved dictionary of string keys and string values.
     *
     * @return the dictionary.
     */
    public Map<String, String> valueDictionaryStringString() {
      return responses.stream()
          .filter(r -> r instanceof CacheDictionaryGetFieldResponse.Hit)
          .collect(
              Collectors.toMap(
                  r -> ((CacheDictionaryGetFieldResponse.Hit) r).fieldString(),
                  r -> ((CacheDictionaryGetFieldResponse.Hit) r).valueString()));
    }

    /**
     * Gets the retrieved dictionary of string keys and string values.
     *
     * @return the dictionary.
     */
    public Map<String, String> valueDictionary() {
      return valueDictionaryStringString();
    }

    /**
     * Gets the retrieved dictionary of string keys and byte array values.
     *
     * @return the dictionary.
     */
    public Map<String, byte[]> valueDictionaryStringBytes() {
      return responses.stream()
          .filter(r -> r instanceof CacheDictionaryGetFieldResponse.Hit)
          .collect(
              Collectors.toMap(
                  r -> ((CacheDictionaryGetFieldResponse.Hit) r).fieldString(),
                  r -> ((CacheDictionaryGetFieldResponse.Hit) r).valueByteArray()));
    }

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

  /**
   * A successful cache dictionary get fields operation for a key that does not exist in the
   * dictionary.
   */
  class Miss implements CacheDictionaryGetFieldsResponse {}

  /**
   * A failed cache dictionary get fields operation. The response itself is an exception, so it can
   * be directly thrown, or the cause of the error can be retrieved with {@link #getCause()}. The
   * message is a copy of the message of the cause.
   */
  class Error extends SdkException implements CacheDictionaryGetFieldsResponse {

    /**
     * Constructs a cache dictionary get fields error with a cause.
     *
     * @param cause the cause.
     */
    public Error(SdkException cause) {
      super(cause);
    }
  }
}
