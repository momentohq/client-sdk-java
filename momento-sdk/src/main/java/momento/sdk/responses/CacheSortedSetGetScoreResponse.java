package momento.sdk.responses;

import com.google.protobuf.ByteString;
import momento.sdk.exceptions.SdkException;

/** Response for a sorted set get score operation */
public interface CacheSortedSetGetScoreResponse {

  /** A successful sorted set get score operation where an element was found. */
  class Hit implements CacheSortedSetGetScoreResponse {

    final ByteString element;
    final double score;

    /**
     * Constructs a sorted set get score response with an element and its score.
     *
     * @param element the element.
     * @param score the retrieved score.
     */
    public Hit(ByteString element, double score) {
      this.element = element;
      this.score = score;
    }

    /**
     * Gets the element as a UTF-8 string.
     *
     * @return the element.
     */
    public String element() {
      return this.element.toStringUtf8();
    }

    /**
     * Gets the element as a byte array.
     *
     * @return the element.
     */
    public byte[] elementByteArray() {
      return this.element.toByteArray();
    }

    /**
     * Gets the score of the element.
     *
     * @return the score.
     */
    public double score() {
      return this.score;
    }

    @Override
    public String toString() {
      return super.toString()
          + ": element: \""
          + element.toString()
          + "\" score: \""
          + score
          + "\"";
    }
  }

  /** A successful sorted set get score operation where no score was found. */
  class Miss implements CacheSortedSetGetScoreResponse {
    final ByteString element;

    /**
     * Constructs a sorted set get score response where the element had no score.
     *
     * @param element the unscored element.
     */
    public Miss(ByteString element) {
      this.element = element;
    }

    /**
     * Gets the element as a UTF-8 string.
     *
     * @return the element.
     */
    public String element() {
      return this.element.toStringUtf8();
    }

    /**
     * Gets the element as a byte array.
     *
     * @return the element.
     */
    public byte[] elementByteArray() {
      return this.element.toByteArray();
    }

    @Override
    public String toString() {
      return super.toString() + ": element: \"" + element + "\"";
    }
  }

  /**
   * A failed sorted set get score operation. The response itself is an exception, so it can be
   * directly thrown, or the cause of the error can be retrieved with {@link #getCause()}. The
   * message is a copy of the message of the cause.
   */
  class Error extends SdkException implements CacheSortedSetGetScoreResponse {

    /**
     * Constructs a sorted set get score error with a cause.
     *
     * @param cause the cause.
     */
    public Error(SdkException cause) {
      super(cause);
    }
  }
}
