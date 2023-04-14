package momento.sdk.messages;

import momento.sdk.exceptions.SdkException;

/** Response for a dictionary increment operation */
public interface CacheDictionaryIncrementResponse {
  /** A successful dictionary increment operation. */
  class Success implements CacheDictionaryIncrementResponse {
    private final int value;

    /**
     * Constructs a dictionary increment success with the incremented value.
     *
     * @param value the incremented value.
     */
    public Success(int value) {
      super();
      this.value = value;
    }

    /**
     * Gets the newly incremented value.
     *
     * @return the value.
     */
    public int valueNumber() {
      return this.value;
    }

    @Override
    public String toString() {
      return String.format("%s: value %d", super.toString(), this.valueNumber());
    }
  }

  /**
   * A failed dictionary increment operation. The response itself is an exception, so it can be
   * directly thrown, or the cause of the error can be retrieved with {@link #getCause()}. The
   * message is a copy of the message of the cause.
   */
  class Error extends SdkException implements CacheDictionaryIncrementResponse {

    /**
     * Constructs a dictionary increment error with a cause.
     *
     * @param cause the cause.
     */
    public Error(SdkException cause) {
      super(cause);
    }
  }
}
