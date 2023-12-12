package momento.sdk;

import momento.sdk.responses.topic.SubscriptionState;
import momento.sdk.responses.topic.TopicMessage;
import momento.sdk.responses.topic.TopicSubscribeResponse;

public class SendSubscribeOptions implements ISubscriptionCallbacks {
    public String cacheName;
    public String topicName;
    public ItemCallback onItem;
    public CompletedCallback onCompleted;
    public ErrorCallback onError;
    public SubscriptionState subscriptionState;
    public TopicSubscribeResponse.Subscription subscription;

    public SendSubscribeOptions(
            String cacheName,
            String topicName,
            ItemCallback onItem,
            CompletedCallback onCompleted,
            ErrorCallback onError,
            SubscriptionState subscriptionState,
            TopicSubscribeResponse.Subscription subscription) {
        this.cacheName = cacheName;
        this.topicName = topicName;
        this.onItem = onItem;
        this.onCompleted = onCompleted;
        this.onError = onError;
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

    public SubscriptionState getSubscriptionState() {
        return subscriptionState;
    }

    public TopicSubscribeResponse.Subscription getSubscription() {
        return subscription;
    }

    @Override
    public void onItem(TopicMessage message) {
        // Implementation of onItem in SendSubscribeOptions
        onItem.onItem(message);
    }

    @Override
    public void onCompleted() {
        // Implementation of onCompleted in SendSubscribeOptions
        onCompleted.onCompleted();
    }

    @Override
    public void onError(Throwable t) {
        // Implementation of onError in SendSubscribeOptions
        onError.onError(t);
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
}
