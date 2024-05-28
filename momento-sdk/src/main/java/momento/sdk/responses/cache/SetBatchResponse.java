package momento.sdk.responses.cache;

import java.util.Map;
import momento.sdk.exceptions.SdkException;

/** Response for a cache set batch operation */
public interface SetBatchResponse {

  /** A successful set batch. */
  class Success implements SetBatchResponse {
    private final Map<String, SetResponse> responses;

    /**
     * Constructs a set batch success.
     *
     * @param responses the individual set responses.
     */
    public Success(Map<String, SetResponse> responses) {
      this.responses = responses;
    }

    /**
     * Gets a map of keys to their individual set responses.
     *
     * @return the keys to responses map.
     */
    public Map<String, SetResponse> results() {
      return responses;
    }
  }

  /**
   * A failed set batch operation. The response itself is an exception, so it can be directly thrown, or
   * the cause of the error can be retrieved with {@link #getCause()}. The message is a copy of the
   * message of the cause.
   */
  class Error extends SdkException implements SetBatchResponse {

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
