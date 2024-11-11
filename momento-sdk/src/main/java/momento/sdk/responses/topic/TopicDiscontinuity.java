package momento.sdk.responses.topic;

public class TopicDiscontinuity {
  private final Integer lastSequenceNumber;
  private final Integer newSequenceNumber;
  private final Integer newSequencePage;

  public TopicDiscontinuity(
      Integer lastSequenceNumber, Integer newSequenceNumber, Integer newSequencePage) {
    this.lastSequenceNumber = lastSequenceNumber;
    this.newSequenceNumber = newSequenceNumber;
    this.newSequencePage = newSequencePage;
  }

  @Override
  public String toString() {
    return "TopicDiscontinuity{"
        + "lastSequenceNumber="
        + lastSequenceNumber
        + ", newSequenceNumber="
        + newSequenceNumber
        + ", newSequencePage="
        + newSequencePage
        + '}';
  }

  /*
   * Gets the last sequence number.
   */
  public Integer getLastSequenceNumber() {
    return lastSequenceNumber;
  }

  /*
   * Gets the new sequence number.
   */
  public Integer getNewSequenceNumber() {
    return newSequenceNumber;
  }

  /*
   * Gets the new sequence page.
   */
  public Integer getNewSequencePage() {
    return newSequencePage;
  }
}
