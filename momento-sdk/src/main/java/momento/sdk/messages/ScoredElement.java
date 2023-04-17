package momento.sdk.messages;

import com.google.protobuf.ByteString;
import java.util.Objects;
import javax.annotation.Nonnull;

/**
 * A pairing of a String/byte[] element to a score. ScoredElements have equality based on their
 * element and ordering based on their score.
 */
public class ScoredElement implements Comparable<ScoredElement> {
  private final ByteString element;
  private final double score;

  /**
   * Constructs a ScoredElement with an element and a score.
   *
   * @param element The element.
   * @param score The element's score.
   */
  public ScoredElement(@Nonnull ByteString element, double score) {
    this.element = element;
    this.score = score;
  }

  /**
   * Gets the element as a String.
   *
   * @return the String element
   */
  public String getElement() {
    return element.toStringUtf8();
  }

  /**
   * Gets the element as a byte array.
   *
   * @return the byte[] element
   */
  public byte[] getElementByteArray() {
    return element.toByteArray();
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
    return this.element.equals(that.element);
  }

  @Override
  public int hashCode() {
    return Objects.hash(element);
  }
}
