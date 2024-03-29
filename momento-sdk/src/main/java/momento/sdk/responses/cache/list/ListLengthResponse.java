package momento.sdk.responses.cache.list;

import momento.sdk.exceptions.SdkException;

/** Response for a list length operation */
public interface ListLengthResponse {

  /** A successful list length operation. */
  class Hit implements ListLengthResponse {
    private final int listLength;

    /**
     * Constructs a list length hit with the length.
     *
     * @param listLength The length of the list.
     */
    public Hit(int listLength) {
      this.listLength = listLength;
    }

    /**
     * Gets the length of the list.
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

  /** A successful list length operation for a list that was not found. */
  class Miss implements ListLengthResponse {}

  /**
   * A failed list length operation. The response itself is an exception, so it can be directly
   * thrown, or the cause of the error can be retrieved with {@link #getClass()} ()}. The message is
   * a copy of the message of the cause.
   */
  class Error extends SdkException implements ListLengthResponse {

    /**
     * Constructs a list length error with a cause.
     *
     * @param cause the cause.
     */
    public Error(SdkException cause) {
      super(cause);
    }
  }
}
