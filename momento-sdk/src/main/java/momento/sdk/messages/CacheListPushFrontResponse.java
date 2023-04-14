package momento.sdk.messages;

import momento.sdk.exceptions.SdkException;

/**
 * Parent response type for a list push front request. The response object is resolved to a
 * type-safe object of one of the following subtypes:
 *
 * <p>{Success}, {Error}
 */
public interface CacheListPushFrontResponse {
  /** A successful list push front operation. */
  class Success implements CacheListPushFrontResponse {
    private final int listLength;

    /**
     * Constructs a list push front success with the new list length.
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
     * @return the list length.
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
   * A failed list push front operation. The response itself is an exception, so it can be directly
   * thrown, or the cause of the error can be retrieved with {@link #getClass()} ()}. The message is
   * a copy of the message of the cause.
   */
  class Error extends SdkException implements CacheListPushFrontResponse {

    /**
     * Constructs a list push front error with a cause.
     *
     * @param cause the cause.
     */
    public Error(SdkException cause) {
      super(cause);
    }
  }
}
