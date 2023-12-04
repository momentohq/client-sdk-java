package momento.sdk.responses.topic;

public class SubscriptionState {
  private Runnable unsubscribeFn;
  private Integer lastTopicSequenceNumber;
  private boolean isSubscribed;

  public SubscriptionState() {
    this.unsubscribeFn = () -> {};
    this.isSubscribed = false;
  }

  public int getResumeAtTopicSequenceNumber() {
    return (lastTopicSequenceNumber != null ? lastTopicSequenceNumber : -1) + 1;
  }

  public void setSubscribed() {
    this.isSubscribed = true;
  }

  public void setUnsubscribed() {
    this.isSubscribed = false;
  }

  public boolean isSubscribed() {
    return isSubscribed;
  }

  public void setUnsubscribeFn(Runnable unsubscribeFn) {
    this.unsubscribeFn = unsubscribeFn;
  }

  public void unsubscribe() {
    if (isSubscribed) {
      unsubscribeFn.run();
      setUnsubscribed();
    }
  }
}
