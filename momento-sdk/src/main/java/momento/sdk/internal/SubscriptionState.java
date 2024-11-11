package momento.sdk.internal;

/** Represents the state of a subscription to a topic. */
public class SubscriptionState {

  private Runnable unsubscribeFn;
  private Integer lastTopicSequenceNumber;
  private Integer lastTopicSequencePage;
  private boolean isSubscribed;

  /** Constructs a new SubscriptionState instance with default values. */
  public SubscriptionState() {
    this.unsubscribeFn = () -> {};
    this.isSubscribed = false;
  }

  /**
   * Gets the topic sequence number to resume the subscription from.
   *
   * @return The topic sequence number to resume from.
   */
  public int getResumeAtTopicSequenceNumber() {
    return (lastTopicSequenceNumber != null ? lastTopicSequenceNumber : -1) + 1;
  }

  /** Sets the topic sequence number to resume the subscription from. */
  public void setResumeAtTopicSequenceNumber(int lastTopicSequenceNumber) {
    this.lastTopicSequenceNumber = lastTopicSequenceNumber;
  }

    /**
     * Gets the topic sequence page to resume the subscription from.
     *
     * @return The topic sequence page to resume from.
     */
  public int getResumeAtTopicSequencePage() {
    return lastTopicSequencePage != null ? lastTopicSequencePage : 0;
  }

  /** Sets the topic sequence page to resume the subscription from. */
  public void setResumeAtTopicSequencePage(int lastTopicSequencePage) {
    this.lastTopicSequencePage = lastTopicSequencePage;
  }

  /** Sets the subscription state to "subscribed." */
  public void setSubscribed() {
    this.isSubscribed = true;
  }

  /**
   * Sets the function to be executed when unsubscribing.
   *
   * @param unsubscribeFn The function to execute when unsubscribing.
   */
  public void setUnsubscribeFn(Runnable unsubscribeFn) {
    this.unsubscribeFn = unsubscribeFn;
  }

  /** Unsubscribes from the topic, executing the unsubscribe function. */
  public void unsubscribe() {
    if (isSubscribed) {
      unsubscribeFn.run();
      this.isSubscribed = false;
    }
  }
}
