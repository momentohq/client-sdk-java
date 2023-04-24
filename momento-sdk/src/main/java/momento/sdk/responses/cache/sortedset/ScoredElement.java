package momento.sdk.responses.cache.sortedset;

import com.google.protobuf.ByteString;
import java.util.Objects;
import javax.annotation.Nonnull;

/**
 * A pairing of a String/byte[] element to a score. ScoredElements have equality based on their
 * element and ordering based on their score.
 */
public class ScoredElement implements Comparable<ScoredElement> {
  private final ByteString valueByteString;
  private final double score;

  /**
   * Constructs a ScoredElement with an element and a score.
   *
   * @param value The element's value.
   * @param score The element's score.
   */
  public ScoredElement(@Nonnull ByteString value, double score) {
    this.valueByteString = value;
    this.score = score;
  }

  /**
   * Constructs a ScoredElement with an element and a score.
   *
   * @param value The element's value.
   * @param score The element's score.
   */
  public ScoredElement(@Nonnull String value, double score) {
    this.valueByteString = ByteString.copyFromUtf8(value);
    this.score = score;
  }

  /**
   * Constructs a ScoredElement with an element and a score.
   *
   * @param value The element's value.
   * @param score The element's score.
   */
  public ScoredElement(@Nonnull byte[] value, double score) {
    this.valueByteString = ByteString.copyFrom(value);
    this.score = score;
  }

  /**
   * Gets the value.
   *
   * @return the value
   */
  public ByteString getValueByteString() {
    return valueByteString;
  }

  /**
   * Gets the value as a String.
   *
   * @return the String element
   */
  public String getValue() {
    return valueByteString.toStringUtf8();
  }

  /**
   * Gets the value as a byte array.
   *
   * @return the byte[] element
   */
  public byte[] getElementByteArray() {
    return valueByteString.toByteArray();
  }

  /**
   * Gets the score.
   *
   * @return the score
   */
  public double getScore() {
    return score;
  }

  @Override
  public int compareTo(@Nonnull ScoredElement that) {
    if (this == that) {
      return 0;
    }
    return Double.compare(this.score, that.score);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final ScoredElement that = (ScoredElement) o;
    return this.valueByteString.equals(that.valueByteString);
  }

  @Override
  public int hashCode() {
    return Objects.hash(valueByteString);
  }
}
