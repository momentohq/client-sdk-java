package momento.sdk.responses.cache.dictionary;

import momento.sdk.exceptions.SdkException;

/** Response for a dictionary increment operation */
public interface DictionaryIncrementResponse {
  /** A successful dictionary increment operation. */
  class Success implements DictionaryIncrementResponse {
    private final int value;

    /**
     * Constructs a dictionary increment success with the incremented value.
     *
     * @param value The incremented value.
     */
    public Success(int value) {
      this.value = value;
    }

    /**
     * Gets the newly incremented value.
     *
     * @return The value.
     */
    public int value() {
      return this.value;
    }

    @Override
    public String toString() {
      return String.format("%s: value %d", super.toString(), this.value());
    }
  }

  /**
   * A failed dictionary increment operation. The response itself is an exception, so it can be
   * directly thrown, or the cause of the error can be retrieved with {@link #getCause()}. The
   * message is a copy of the message of the cause.
   */
  class Error extends SdkException implements DictionaryIncrementResponse {

    /**
     * Constructs a dictionary increment error with a cause.
     *
     * @param cause The cause.
     */
    public Error(SdkException cause) {
      super(cause);
    }
  }
}
