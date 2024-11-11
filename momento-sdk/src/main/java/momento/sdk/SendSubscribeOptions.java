package momento.sdk;

import momento.sdk.internal.SubscriptionState;
import momento.sdk.responses.topic.TopicDiscontinuity;
import momento.sdk.responses.topic.TopicMessage;
import momento.sdk.responses.topic.TopicSubscribeResponse;

class SendSubscribeOptions implements ISubscriptionCallbacks {
  String cacheName;
  String topicName;
  ItemCallback onItem;
  CompletedCallback onCompleted;
  ErrorCallback onError;
  DiscontinuityCallback onDiscontinuity;
  HeartbeatCallback onHeartbeat;
  ConnectionLostCallback onConnectionLost;
  ConnectionRestoredCallback onConnectionRestored;
  SubscriptionState subscriptionState;
  TopicSubscribeResponse.Subscription subscription;

  SendSubscribeOptions(
      String cacheName,
      String topicName,
      ItemCallback onItem,
      CompletedCallback onCompleted,
      ErrorCallback onError,
      DiscontinuityCallback onDiscontinuity,
      HeartbeatCallback onHeartbeat,
      ConnectionLostCallback onConnectionLost,
      ConnectionRestoredCallback onConnectionRestored,
      SubscriptionState subscriptionState,
      TopicSubscribeResponse.Subscription subscription) {
    this.cacheName = cacheName;
    this.topicName = topicName;
    this.onItem = onItem;
    this.onCompleted = onCompleted;
    this.onError = onError;
    this.onDiscontinuity = onDiscontinuity;
    this.onHeartbeat = onHeartbeat;
    this.onConnectionLost = onConnectionLost;
    this.onConnectionRestored = onConnectionRestored;
    this.subscriptionState = subscriptionState;
    this.subscription = subscription;
  }

  public String getCacheName() {
    return cacheName;
  }

  public String getTopicName() {
    return topicName;
  }

  public ItemCallback getOnItem() {
    return onItem;
  }

  public CompletedCallback getOnCompleted() {
    return onCompleted;
  }

  public ErrorCallback getOnError() {
    return onError;
  }

  public DiscontinuityCallback getOnDiscontinuity() {
    return onDiscontinuity;
  }

  public HeartbeatCallback getOnHeartbeat() {
    return onHeartbeat;
  }

  public SubscriptionState getSubscriptionState() {
    return subscriptionState;
  }

  public TopicSubscribeResponse.Subscription getSubscription() {
    return subscription;
  }

  @Override
  public void onItem(TopicMessage message) {
    onItem.onItem(message);
  }

  @Override
  public void onCompleted() {
    onCompleted.onCompleted();
  }

  @Override
  public void onError(Throwable t) {
    onError.onError(t);
  }

  @Override
  public void onDiscontinuity(TopicDiscontinuity discontinuity) {
    onDiscontinuity.onDiscontinuity(discontinuity);
  }

  @Override
  public void onHeartbeat() {
    onHeartbeat.onHeartbeat();
  }

  @Override
  public void onConnectionLost() {
    onConnectionLost.onConnectionLost();
  }

  @Override
  public void onConnectionRestored() {
    onConnectionRestored.onConnectionRestored();
  }

  @FunctionalInterface
  public interface ItemCallback {
    void onItem(TopicMessage message);
  }

  @FunctionalInterface
  public interface CompletedCallback {
    void onCompleted();
  }

  @FunctionalInterface
  public interface ErrorCallback {
    void onError(Throwable t);
  }

  @FunctionalInterface
  public interface DiscontinuityCallback {
    void onDiscontinuity(TopicDiscontinuity discontinuity);
  }

  @FunctionalInterface
  public interface HeartbeatCallback {
    void onHeartbeat();
  }

  @FunctionalInterface
  public interface ConnectionLostCallback {
    void onConnectionLost();
  }

  @FunctionalInterface
  public interface ConnectionRestoredCallback {
    void onConnectionRestored();
  }
}
