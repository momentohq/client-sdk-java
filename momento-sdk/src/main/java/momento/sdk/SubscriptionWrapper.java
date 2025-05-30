package momento.sdk;

import com.google.protobuf.ByteString;
import grpc.cache_client.pubsub._SubscriptionItem;
import grpc.cache_client.pubsub._SubscriptionRequest;
import grpc.cache_client.pubsub._TopicItem;
import grpc.cache_client.pubsub._TopicValue;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

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
import momento.sdk.exceptions.TimeoutException;
import momento.sdk.exceptions.UnknownException;
import momento.sdk.internal.MomentoGrpcErrorDetails;
import momento.sdk.internal.MomentoTransportErrorDetails;
import momento.sdk.internal.SubscriptionState;
import momento.sdk.responses.topic.TopicDiscontinuity;
import momento.sdk.responses.topic.TopicMessage;
import momento.sdk.responses.topic.TopicSubscribeResponse;
import momento.sdk.retry.SubscriptionRetryStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SubscriptionWrapper implements Closeable {
  private static final Logger logger = LoggerFactory.getLogger(SubscriptionWrapper.class);
  private final long requestTimeoutSeconds;
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
      long requestTimeoutSeconds,
      SubscriptionRetryStrategy retryStrategy) {
    this.cacheName = cacheName;
    this.topicName = topicName;
    this.connection = connection;
    this.callbacks = callbacks;
    this.subscriptionState = subscriptionState;
    this.requestTimeoutSeconds = requestTimeoutSeconds;
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
    final CompletableFuture<Void> firstMessageTimeoutFuture = new CompletableFuture<>();

    // isSubscribed is true by default and is set to false only when unsubscribe is called.
    // Do not allow resubscribe attempt on a subscription that is ending.
    if (!isSubscribed.get()) {
      completeExceptionally(
          future, new UnknownException("Cannot subscribe to an unsubscribed subscription"));
      return future;
    }

    scheduler.schedule(
        () -> {
          if (!firstMessageTimeoutFuture.isDone()) {
            logger.warn(
                "First message timeout exceeded for topic {} on cache {}", topicName, cacheName);

            if (subscription != null) {
              subscription.get().cancel("Timed out waiting for first message", null);
            }

            firstMessageTimeoutFuture.completeExceptionally(
                new TimeoutException(
                    new RuntimeException(
                        "Timed out waiting for first message (heartbeat) for topic "
                            + topicName
                            + " on cache "
                            + cacheName),
                    new MomentoTransportErrorDetails(
                        new MomentoGrpcErrorDetails(
                            Status.Code.DEADLINE_EXCEEDED,
                            "Timed out waiting for first message (heartbeat)",
                            null))));
          }
        },
        requestTimeoutSeconds,
        TimeUnit.SECONDS);

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
              if (firstMessageTimeoutFuture != null) {
                firstMessageTimeoutFuture.complete(null);
              }
              future.complete(null);
              return;
            }
            if (isConnectionLost.compareAndSet(true, false)) {
              callbacks.onConnectionRestored();
            }
            handleSubscriptionItem(item);
          }

          @Override
          public void onError(Throwable t) {
            if (firstMessage.get()) {
              if (firstMessageTimeoutFuture != null && !firstMessageTimeoutFuture.isDone()) {
                firstMessageTimeoutFuture.completeExceptionally(t);
              }
              if (!future.isDone()) {
                future.completeExceptionally(t);
              }
              completeExceptionally(future, t);
            } else {
              logger.debug("Subscription failed, retrying...");
              if (isConnectionLost.compareAndSet(false, true)) {
                callbacks.onConnectionLost();
              }

              // If it was a CANCELLED error because unsubscribe was called, do not retry and
              // exit gracefully.
              if (t instanceof StatusRuntimeException) {
                final StatusRuntimeException exception = (StatusRuntimeException) t;
                if (exception.getStatus().getCode() == Status.Code.CANCELLED && exception.getMessage().contains("Unsubscribing")) {
                  callbacks.onCompleted();
                  close();
                  return;
                }
              }

              // Otherwise, determine if we should retry.
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

    // Combine the subscription future and the first-message timeout future.
    // Although CompletableFuture.anyOf(...) returns as soon as *either* future completes,
    // it does not tell us *which one* completed, nor does it propagate the actual exception.
    // So we explicitly check both futures:
    //
    // - If the timeout future completed exceptionally, that means the client didn't receive
    //   the first message (typically a heartbeat) within the expected time, so we want to return
    //   that timeout error.
    //
    // - If the timeout didn't fire, but the subscription future failed (e.g., gRPC UNAVAILABLE),
    //   then we propagate that error.
    //
    // - If neither completed exceptionally, it means the subscription succeeded, and we return
    // success.
    //
    // This ensures that a timeout error always takes precedence and prevents it from being
    // overwritten by a later gRPC error (e.g., UNAVAILABLE) that may arrive after the timeout has
    // fired.
    CompletableFuture<Void> result = new CompletableFuture<>();
    CompletableFuture.anyOf(future, firstMessageTimeoutFuture)
        .whenComplete(
            (ignored, throwable) -> {
              if (firstMessageTimeoutFuture.isCompletedExceptionally()) {
                firstMessageTimeoutFuture.whenComplete(
                    (__, ex) -> result.completeExceptionally(ex));
              } else if (future.isCompletedExceptionally()) {
                future.whenComplete((__, ex) -> result.completeExceptionally(ex));
              } else {
                result.complete(null);
              }
            });
    return result;
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
