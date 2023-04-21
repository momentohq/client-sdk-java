package momento.sdk.responses;

import momento.sdk.exceptions.SdkException;

/**
 * Parent response type for a list push back request. The response object is resolved to a type-safe
 * object of one of the following subtypes:
 *
 * <p>{Success}, {Error}
 */
public interface CacheListPushBackResponse {
  /** A successful list push back operation. */
  class Success implements CacheListPushBackResponse {
    private final int listLength;

    /**
     * Constructs a list push back success with the new list length.
     *
     * @param listLength The new length of the list.
     */
    public Success(int listLength) {
      super();
      this.listLength = listLength;
    }

    /**
     * Gets the new length of the list.
     *
     * @return The list length.
     */
    public int getListLength() {
      return this.listLength;
    }

    @Override
    public String toString() {
      return String.format("%s: value %d", super.toString(), this.getListLength());
    }
  }

  /**
   * A failed list push back operation. The response itself is an exception, so it can be directly
   * thrown, or the cause of the error can be retrieved with {@link #getClass()} ()}. The message is
   * a copy of the message of the cause.
   */
  class Error extends SdkException implements CacheListPushBackResponse {

    /**
     * Constructs a list push back error with a cause.
     *
     * @param cause the cause.
     */
    public Error(SdkException cause) {
      super(cause);
    }
  }
}
