package momento.sdk;

import momento.sdk.responses.topic.TopicMessage;

/** Represents options for a topic subscription callback. */
public interface ISubscribeCallOptions {
  /**
   * Called when a new message is received on the subscribed topic.
   *
   * @param message The received topic message.
   */
  void onItem(TopicMessage message);

  /** Called when the subscription is successfully completed. */
  void onCompleted();

  /**
   * Called when an error occurs during the subscription.
   *
   * @param t The throwable representing the error.
   */
  void onError(Throwable t);
}
