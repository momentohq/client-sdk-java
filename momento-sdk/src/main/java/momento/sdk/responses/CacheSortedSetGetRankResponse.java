package momento.sdk.responses;

import momento.sdk.exceptions.SdkException;

/** Response for a sorted set get rank operation */
public interface CacheSortedSetGetRankResponse {

  /** A successful sorted set get rank operation where an element was found. */
  class Hit implements CacheSortedSetGetRankResponse {
    private final long rank;

    /**
     * Constructs a sorted set get rank response with a rank.
     *
     * @param rank the retrieved rank.
     */
    public Hit(long rank) {
      this.rank = rank;
    }

    /**
     * Gets the rank of the element.
     *
     * @return the rank.
     */
    public long rank() {
      return this.rank;
    }

    @Override
    public String toString() {
      return super.toString() + ": rank: \"" + rank + "\"";
    }
  }

  /** A successful sorted set get rank operation where no element was found. */
  class Miss implements CacheSortedSetGetRankResponse {}

  /**
   * A failed sorted set get rank operation. The response itself is an exception, so it can be
   * directly thrown, or the cause of the error can be retrieved with {@link #getCause()}. The
   * message is a copy of the message of the cause.
   */
  class Error extends SdkException implements CacheSortedSetGetRankResponse {

    /**
     * Constructs a sorted set get rank error with a cause.
     *
     * @param cause the cause.
     */
    public Error(SdkException cause) {
      super(cause);
    }
  }
}
