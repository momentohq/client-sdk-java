package momento.sdk;

import grpc.cache_client.pubsub._SubscriptionItem;
import grpc.cache_client.pubsub._SubscriptionRequest;

/** Represents a connection to an ScsTopic for subscribing to events. */
interface IScsTopicConnection {

  /**
   * Closes the connection.
   *
   * <p>Note: This method is intended for testing purposes and should never be called from outside
   * of tests.
   */
  default void close() {}

  /**
   * Opens the connection.
   *
   * <p>Note: This method is intended for testing purposes and should never be called from outside
   * of tests.
   */
  default void open() {}

  /**
   * Subscribes to a specific topic using the provided subscription request and observer.
   *
   * @param subscriptionRequest The subscription request containing details about the subscription.
   * @param subscription The observer to handle incoming subscription items.
   */
  void subscribe(
      _SubscriptionRequest subscriptionRequest,
      CancelableClientCallStreamObserver<_SubscriptionItem> subscription);
}
