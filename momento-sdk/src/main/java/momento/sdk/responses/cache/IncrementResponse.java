package momento.sdk.responses.cache;

import momento.sdk.exceptions.SdkException;

/** Response for a cache increment operation */
public interface IncrementResponse {

  /** A successful cache increment operation. */
  class Success implements IncrementResponse {
    private final int value;

    /**
     * Constructs a cache increment success with the incremented value.
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
   * A failed cache increment operation. The response itself is an exception, so it can be directly
   * thrown, or the cause of the error can be retrieved with {@link #getClass()} ()}. The message is
   * a copy of the message of the cause.
   */
  class Error extends SdkException implements IncrementResponse {

    /**
     * Constructs a cache increment error with a cause.
     *
     * @param cause The cause.
     */
    public Error(SdkException cause) {
      super(cause);
    }
  }
}
