package momento.sdk;

import com.google.protobuf.ByteString;
import grpc.cache_client.pubsub._SubscriptionItem;
import grpc.cache_client.pubsub._SubscriptionRequest;
import grpc.cache_client.pubsub._TopicItem;
import grpc.cache_client.pubsub._TopicValue;
import java.io.Closeable;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import momento.sdk.exceptions.CacheServiceExceptionMapper;
import momento.sdk.exceptions.InternalServerException;
import momento.sdk.exceptions.UnknownException;
import momento.sdk.internal.SubscriptionState;
import momento.sdk.responses.topic.TopicDiscontinuity;
import momento.sdk.responses.topic.TopicMessage;
import momento.sdk.responses.topic.TopicSubscribeResponse;
import momento.sdk.retry.SubscriptionRetryStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SubscriptionWrapper implements Closeable {
  private static final Logger logger = LoggerFactory.getLogger(SubscriptionWrapper.class);

  private final IScsTopicConnection connection;
  private final String cacheName;
  private final String topicName;
  private final ISubscriptionCallbacks callbacks;
  private final SubscriptionState subscriptionState;
  private final SubscriptionRetryStrategy retryStrategy;
  private final AtomicBoolean firstMessage = new AtomicBoolean(true);
  private final AtomicBoolean isConnectionLost = new AtomicBoolean(false);
  private final AtomicBoolean isSubscribed = new AtomicBoolean(true);

  // TODO: share a thread pool across subscription wrappers for retries and potentially for
  // callbacks
  private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

  private final AtomicReference<CancelableClientCallStreamObserver<_SubscriptionItem>>
      subscription = new AtomicReference<>();

  SubscriptionWrapper(
      String cacheName,
      String topicName,
      IScsTopicConnection connection,
      ISubscriptionCallbacks callbacks,
      SubscriptionState subscriptionState,
      SubscriptionRetryStrategy retryStrategy) {
    this.cacheName = cacheName;
    this.topicName = topicName;
    this.connection = connection;
    this.callbacks = callbacks;
    this.subscriptionState = subscriptionState;
    this.retryStrategy = retryStrategy;
  }

  /**
   * This method returns a CompletableFuture that represents the asynchronous execution of the
   * internal subscription logic with retry mechanism.
   *
   * @return A CompletableFuture representing the asynchronous execution of the internal
   *     subscription logic with retry mechanism.
   */
  CompletableFuture<Void> subscribeWithRetry() {
    final CompletableFuture<Void> future = new CompletableFuture<>();

    if (!isSubscribed.get()) {
      completeExceptionally(
          future, new UnknownException("Cannot subscribe to an unsubscribed subscription"));
      return future;
    }

    final CancelableClientCallStreamObserver<_SubscriptionItem> observer =
        new CancelableClientCallStreamObserver<_SubscriptionItem>() {
          @Override
          public void onNext(_SubscriptionItem item) {
            if (firstMessage.compareAndSet(true, false)) {
              if (item.getKindCase() != _SubscriptionItem.KindCase.HEARTBEAT) {
                completeExceptionally(
                    future,
                    new InternalServerException(
                        "Expected heartbeat message for topic "
                            + topicName
                            + " on cache "
                            + cacheName
                            + ". Got: "
                            + item.getKindCase()));
              }
              future.complete(null);
            }
            if (isConnectionLost.compareAndSet(true, false)) {
              callbacks.onConnectionRestored();
            }
            handleSubscriptionItem(item);
          }

          @Override
          public void onError(Throwable t) {
            if (firstMessage.get()) {
              completeExceptionally(future, t);
            } else {
              logger.debug("Subscription failed, retrying...");
              if (isConnectionLost.compareAndSet(false, true)) {
                callbacks.onConnectionLost();
              }
              final Optional<Duration> retryOpt = retryStrategy.determineWhenToRetry(t);
              if (retryOpt.isPresent()) {
                if (isSubscribed.get()) {
                  scheduleRetry(retryOpt.get(), () -> subscribeWithRetry());
                } else {
                  logger.debug("Cannot retry an unsubscribed subscription");
                }
              } else {
                callbacks.onError(t);
                close();
              }
            }
          }

          @Override
          public void onCompleted() {
            handleSubscriptionCompleted();
          }
        };

    final _SubscriptionRequest subscriptionRequest =
        _SubscriptionRequest.newBuilder()
            .setCacheName(cacheName)
            .setTopic(topicName)
            .setResumeAtTopicSequenceNumber(subscriptionState.getResumeAtTopicSequenceNumber())
            .setSequencePage(subscriptionState.getResumeAtTopicSequencePage())
            .build();

    try {
      connection.subscribe(subscriptionRequest, observer);
      subscriptionState.setSubscribed();
    } catch (Exception e) {
      completeExceptionally(future, e);
    }
    subscription.set(observer);

    return future;
  }

  private void completeExceptionally(CompletableFuture<Void> future, Throwable t) {
    future.completeExceptionally(
        new TopicSubscribeResponse.Error(CacheServiceExceptionMapper.convert(t)));
    close();
  }

  private void scheduleRetry(Duration retryDelay, Runnable retryAction) {
    scheduler.schedule(retryAction, retryDelay.toMillis(), TimeUnit.MILLISECONDS);
  }

  private void handleSubscriptionCompleted() {
    callbacks.onCompleted();
  }

  private void handleSubscriptionItem(_SubscriptionItem item) {
    switch (item.getKindCase()) {
      case ITEM:
        handleSubscriptionItemMessage(item);
        break;
      case DISCONTINUITY:
        handleSubscriptionDiscontinuity(item);
        break;
      case HEARTBEAT:
        handleSubscriptionHeartbeat();
        break;
      default:
        handleSubscriptionUnknown();
        break;
    }
  }

  private void handleSubscriptionDiscontinuity(_SubscriptionItem discontinuityItem) {
    logger.trace(
        "discontinuity {}, {}, {}, {}, {}",
        cacheName,
        topicName,
        discontinuityItem.getDiscontinuity().getLastTopicSequence(),
        discontinuityItem.getDiscontinuity().getNewTopicSequence(),
        discontinuityItem.getDiscontinuity().getNewSequencePage());
    subscriptionState.setResumeAtTopicSequenceNumber(
        discontinuityItem.getDiscontinuity().getNewTopicSequence());
    subscriptionState.setResumeAtTopicSequencePage(
        discontinuityItem.getDiscontinuity().getNewSequencePage());
    callbacks.onDiscontinuity(
        new TopicDiscontinuity(
            discontinuityItem.getDiscontinuity().getLastTopicSequence(),
            discontinuityItem.getDiscontinuity().getNewTopicSequence(),
            discontinuityItem.getDiscontinuity().getNewSequencePage()));
  }

  private void handleSubscriptionHeartbeat() {
    logger.trace("heartbeat {} {}", cacheName, topicName);
    callbacks.onHeartbeat();
  }

  private void handleSubscriptionUnknown() {
    logger.warn("unknown {} {}", cacheName, topicName);
  }

  private void handleSubscriptionItemMessage(_SubscriptionItem item) {
    _TopicItem topicItem = item.getItem();
    _TopicValue topicValue = topicItem.getValue();
    subscriptionState.setResumeAtTopicSequenceNumber(topicItem.getTopicSequenceNumber());
    subscriptionState.setResumeAtTopicSequencePage(topicItem.getSequencePage());
    TopicMessage message;

    switch (topicValue.getKindCase()) {
      case TEXT:
        message =
            handleSubscriptionTextMessage(topicValue.getText(), item.getItem().getPublisherId());
        callbacks.onItem(message);
        break;
      case BINARY:
        message =
            handleSubscriptionBinaryMessage(
                topicValue.getBinary().toByteArray(), item.getItem().getPublisherId());
        callbacks.onItem(message);
        break;
      default:
        handleSubscriptionUnknown();
        break;
    }
  }

  private TopicMessage.Text handleSubscriptionTextMessage(String text, String publisherId) {
    _TopicValue topicValue = _TopicValue.newBuilder().setText(text).build();
    return new TopicMessage.Text(topicValue, publisherId.isEmpty() ? null : publisherId);
  }

  private TopicMessage.Binary handleSubscriptionBinaryMessage(byte[] binary, String publisherId) {
    _TopicValue topicValue =
        _TopicValue.newBuilder().setBinary(ByteString.copyFrom(binary)).build();
    return new TopicMessage.Binary(topicValue, publisherId.isEmpty() ? null : publisherId);
  }

  public void unsubscribe() {
    if (isSubscribed.compareAndSet(true, false)) {
      subscription
          .get()
          .cancel("Unsubscribing from topic: " + topicName + " in cache: " + cacheName, null);
      close();
    }
  }

  @Override
  public void close() {
    scheduler.shutdown();
  }
}
