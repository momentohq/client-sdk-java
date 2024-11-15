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

    StringBuilder sb = new StringBuilder();
    sb.append("TopicDiscontinuity{");
    sb.append("lastSequenceNumber=");
    sb.append(lastSequenceNumber);
    sb.append(", newSequenceNumber=");
    sb.append(newSequenceNumber);
    sb.append(", newSequencePage=");
    sb.append(newSequencePage);
    sb.append('}');
    return sb.toString();
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
