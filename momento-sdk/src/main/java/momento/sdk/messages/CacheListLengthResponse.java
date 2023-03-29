package momento.sdk.messages;

import momento.sdk.exceptions.SdkException;

/** Response for a cache length operation */
public interface CacheListLengthResponse {

  class Hit implements CacheListLengthResponse {
    private final int listLength;

    /**
     * Constructs a cache length hit with an encoded value.
     *
     * @param listLength of the list.
     */
    public Hit(int listLength) {
      this.listLength = listLength;
    }

    /**
     * Gets the length of the list
     *
     * @return the listLength.
     */
    public int getListLength() {
      return this.listLength;
    }

    @Override
    public String toString() {
      return String.format("%s: value %d", super.toString(), this.getListLength());
    }
  }

  /** A successful cache length operation for a list that was not found. */
  class Miss implements CacheListLengthResponse {}

  /**
   * A failed list length operation. The response itself is an exception, so it can be directly
   * thrown, or the cause of the error can be retrieved with {@link #getClass()} ()}. The message is
   * a copy of the message of the cause.
   */
  class Error extends SdkException implements CacheListLengthResponse {

    /**
     * Constructs a cache list length error with a cause.
     *
     * @param cause the cause.
     */
    public Error(SdkException cause) {
      super(cause);
    }
  }
}
