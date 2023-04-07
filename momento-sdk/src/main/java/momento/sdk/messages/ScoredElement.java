package momento.sdk.messages;

public class ScoredElement<T> {
  private final T element;
  private final double score;

  public ScoredElement(T element, double score) {
    this.element = element;
    this.score = score;
  }

  public T getElement() {
    return element;
  }

  public double getScore() {
    return score;
  }
}
