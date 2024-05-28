package momento.sdk.responses.cache;

import java.util.Base64;
import java.util.Map;
import java.util.stream.Collectors;
import momento.sdk.exceptions.SdkException;
import momento.sdk.internal.StringHelpers;

/** Response for a cache get batch operation */
public interface GetBatchResponse {

  /** A successful get batch operation for a key that has a value. */
  class Success implements GetBatchResponse {
    private final Map<String, GetResponse> responses;

    /**
     * Constructs a cache get batch success.
     *
     * @param responses the individual get responses.
     */
    public Success(Map<String, GetResponse> responses) {
      this.responses = responses;
    }

    /**
     * Gets a map of retrieved keys to their get responses.
     *
     * @return the keys to responses map.
     */
    public Map<String, GetResponse> results() {
      return responses;
    }

    /**
     * Gets a map of the retrieved keys to their values encoded as UTF-8 {@link String}s. Keys that don't have values
     * aren't included.
     *
     * @return the keys to value strings map.
     */
    public Map<String, String> valueMap() {
      return valueMapStringString();
    }

    /**
     * Gets a map of the retrieved keys to their values encoded as UTF-8 {@link String}s. Keys that don't have values
     * aren't included.
     *
     * @return the keys to value strings map.
     */
    public Map<String, String> valueMapStringString() {
      return responses.entrySet().stream()
          .filter(e -> e.getValue() instanceof GetResponse.Hit)
          .collect(
              Collectors.toMap(
                  Map.Entry::getKey, e -> ((GetResponse.Hit) e.getValue()).valueString()));
    }

    /**
     * Gets a map of the retrieved keys to their values as byte arrays. Keys that don't have values aren't included.
     *
     * @return the keys to value bytes map.
     */
    public Map<String, byte[]> valueMapStringByteArray() {
      return responses.entrySet().stream()
          .filter(e -> e.getValue() instanceof GetResponse.Hit)
          .collect(
              Collectors.toMap(
                  Map.Entry::getKey, e -> ((GetResponse.Hit) e.getValue()).valueByteArray()));
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
   * A failed get batch operation. The response itself is an exception, so it can be directly thrown, or
   * the cause of the error can be retrieved with {@link #getCause()}. The message is a copy of the
   * message of the cause.
   */
  class Error extends SdkException implements GetBatchResponse {

    /**
     * Constructs a cache get error with a cause.
     *
     * @param cause the cause.
     */
    public Error(SdkException cause) {
      super(cause);
    }
  }
}
