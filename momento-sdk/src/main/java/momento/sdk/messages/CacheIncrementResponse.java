package momento.sdk.messages;

import momento.sdk.exceptions.SdkException;

public interface CacheIncrementResponse {

  /** A successful cache increment operation. */
  class Success implements CacheIncrementResponse {
    private int value;

    public Success(int value) {
      super();
      this.value = value;
    }

    public int valueNumber() {
      return this.value;
    }

    @Override
    public String toString() {
      return String.format("%s: value %d", super.toString(), this.valueNumber());
    }
  }

  /**
   * A failed cache increment operation. The response itself is an exception, so it can be
   * directly thrown, or the cause of the error can be retrieved with {@link #getClass()} ()}. The
   * message is a copy of the message of the cause.
   */
  class Error implements CacheIncrementResponse {

    private final SdkException _error;

    public Error(SdkException _error) {
      this._error = _error;
    }
  }
}
