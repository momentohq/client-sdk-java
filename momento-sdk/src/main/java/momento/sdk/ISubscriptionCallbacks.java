package momento.sdk;

import momento.sdk.responses.topic.TopicDiscontinuity;
import momento.sdk.responses.topic.TopicMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Represents options for a topic subscription callback. */
public interface ISubscriptionCallbacks {
  Logger logger = LoggerFactory.getLogger(ISubscriptionCallbacks.class);
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

  /** Called when a discontinuity occurs during the subscription. */
  default void onDiscontinuity(TopicDiscontinuity discontinuity) {}

  /** Called when a heartbeat is received during the subscription. */
  default void onHeartbeat() {}

  /** Called when the connection to the topic is lost. */
  default void onConnectionLost() {
    // logger.info("Connection to topic lost");
  }

  /** Called when the connection to the topic is restored. */
  default void onConnectionRestored() {
    logger.info("Connection to topic restored");
  }
}
