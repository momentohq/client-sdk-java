package momento.sdk.responses.topic;

import momento.sdk.exceptions.SdkException;

/** Represents the response for a topic subscribe operation. */
public interface TopicSubscribeResponse {

  /** Represents a successful topic subscribe operation. */
  class Subscription implements TopicSubscribeResponse {
    private SubscriptionState subscriptionState;

    /**
     * Constructs a Subscription instance with the provided subscription state.
     *
     * @param subscriptionState The subscription state.
     */
    public Subscription(SubscriptionState subscriptionState) {
      super();
      this.subscriptionState = subscriptionState;
    }

    /** Unsubscribes from the topic. */
    public void unsubscribe() {
      this.subscriptionState.unsubscribe();
    }
  }

  /**
   * Represents a failed topic subscribe operation. The response itself is an exception, so it can
   * be directly thrown, or the cause of the error can be retrieved with {@link #getCause()}. The
   * message is a copy of the message of the cause.
   */
  class Error extends SdkException implements TopicSubscribeResponse {

    /**
     * Constructs a topic subscribe error with a cause.
     *
     * @param cause The cause of the error.
     */
    public Error(SdkException cause) {
      super(cause);
    }
  }
}
