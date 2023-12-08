package momento.sdk.responses.topic;

/** Represents the state of a subscription to a topic. */
public class SubscriptionState {

  private Runnable unsubscribeFn;
  private Integer lastTopicSequenceNumber;
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

  /** Sets the subscription state to "subscribed." */
  public void setSubscribed() {
    this.isSubscribed = true;
  }

  /**
   * Checks if the subscription is in a "subscribed" state.
   *
   * @return True if subscribed, false otherwise.
   */
  public boolean isSubscribed() {
    return isSubscribed;
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
