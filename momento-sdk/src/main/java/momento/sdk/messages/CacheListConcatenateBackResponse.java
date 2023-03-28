package momento.sdk.messages;

import momento.sdk.exceptions.SdkException;

/**
 * Parent response type for a list concatenate back request. The response object is resolved to a
 * type-safe object of one of the following subtypes:
 *
 * <p>{Success}, {Error}
 */
public interface CacheListConcatenateBackResponse {
  /** A successful list concatenate back operation. */
  class Success implements CacheListConcatenateBackResponse {
    private int listLength;

    public Success(int listLength) {
      super();
      this.listLength = listLength;
    }

    public int getListLength() {
      return this.listLength;
    }

    @Override
    public String toString() {
      return String.format("%s: value %d", super.toString(), this.getListLength());
    }
  }

  /**
   * A failed list concatenate back operation. The response itself is an exception, so it can be
   * directly thrown, or the cause of the error can be retrieved with {@link #getClass()} ()}. The
   * message is a copy of the message of the cause.
   */
  class Error implements CacheListConcatenateBackResponse {

    private final SdkException _error;

    public Error(SdkException _error) {
      this._error = _error;
    }
  }
}
