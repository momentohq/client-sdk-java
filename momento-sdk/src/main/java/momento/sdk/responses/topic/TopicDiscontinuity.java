package momento.sdk.responses.topic;

public class TopicDiscontinuity {
  private final Long lastSequenceNumber;
  private final Long newSequenceNumber;
  private final Long newSequencePage;

  public TopicDiscontinuity(long lastSequenceNumber, long newSequenceNumber, long newSequencePage) {
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
  public long getLastSequenceNumber() {
    return lastSequenceNumber;
  }

  /*
   * Gets the new sequence number.
   */
  public long getNewSequenceNumber() {
    return newSequenceNumber;
  }

  /*
   * Gets the new sequence page.
   */
  public long getNewSequencePage() {
    return newSequencePage;
  }
}
