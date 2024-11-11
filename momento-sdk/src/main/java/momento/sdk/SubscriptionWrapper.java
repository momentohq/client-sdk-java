package momento.sdk;

import com.google.protobuf.ByteString;
import grpc.cache_client.pubsub._SubscriptionItem;
import grpc.cache_client.pubsub._SubscriptionRequest;
import grpc.cache_client.pubsub._TopicItem;
import grpc.cache_client.pubsub._TopicValue;
import io.grpc.Status;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import momento.sdk.exceptions.CacheServiceExceptionMapper;
import momento.sdk.exceptions.InternalServerException;
import momento.sdk.responses.topic.TopicDiscontinuity;
import momento.sdk.responses.topic.TopicMessage;
import momento.sdk.responses.topic.TopicSubscribeResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SubscriptionWrapper implements AutoCloseable {
  private final Logger logger = LoggerFactory.getLogger(SubscriptionWrapper.class);
  private final IScsTopicConnection connection;
  private final SendSubscribeOptions options;
  private boolean firstMessage = true;
  private boolean isConnectionLost = false;

  private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

  private CancelableClientCallStreamObserver<_SubscriptionItem> subscription;

  SubscriptionWrapper(IScsTopicConnection connection, SendSubscribeOptions options) {
    this.connection = connection;
    this.options = options;
  }

  /**
   * Public method for testing purposes only. Do not call this method in production code or any
   * context other than testing the topic client.
   *
   * <p>This method returns a CompletableFuture that represents the asynchronous execution of the
   * internal subscription logic with retry mechanism.
   *
   * @return A CompletableFuture representing the asynchronous execution of the internal
   *     subscription logic with retry mechanism.
   */
  public CompletableFuture<Void> subscribeWithRetry() {
    CompletableFuture<Void> future = new CompletableFuture<>();
    subscribeWithRetryInternal(future);
    return future;
  }

  private void subscribeWithRetryInternal(CompletableFuture<Void> future) {
    subscription =
        new CancelableClientCallStreamObserver<_SubscriptionItem>() {
          @Override
          public void onNext(_SubscriptionItem item) {
            if (firstMessage) {
              if (item.getKindCase() != _SubscriptionItem.KindCase.HEARTBEAT) {
                future.completeExceptionally(
                    new InternalServerException(
                        "Expected heartbeat message for topic "
                            + options.getTopicName()
                            + " on cache "
                            + options.getCacheName()
                            + ". Got: "
                            + item.getKindCase()));
              }
              firstMessage = false;
              future.complete(null);
            }
            if (isConnectionLost) {
              isConnectionLost = false;
              options.onConnectionRestored();
            }
            handleSubscriptionItem(item);
          }

          @Override
          public void onError(Throwable t) {
            if (firstMessage) {
              firstMessage = false;
              future.completeExceptionally(t);
            } else {
              logger.debug("Subscription failed, retrying...");
              if (!isConnectionLost) {
                isConnectionLost = true;
                options.onConnectionLost();
              }
              if (t instanceof io.grpc.StatusRuntimeException) {
                logger.debug(
                    "Throwable is an instance of StatusRuntimeException, checking status code...");
                io.grpc.StatusRuntimeException statusRuntimeException =
                    (io.grpc.StatusRuntimeException) t;
                if (statusRuntimeException.getStatus().getCode() == Status.Code.UNAVAILABLE) {
                  logger.info("Status code is UNAVAILABLE, retrying subscription after a delay...");
                  scheduleRetry(() -> subscribeWithRetryInternal(future));
                } else {
                  logger.debug("Status code is not UNAVAILABLE, not retrying subscription.");
                }
              } else {
                logger.debug(
                    "Throwable is not an instance of StatusRuntimeException, not retrying subscription.");
                options.onError(t);
              }
            }
          }

          @Override
          public void onCompleted() {
            handleSubscriptionCompleted();
          }
        };

    _SubscriptionRequest subscriptionRequest =
        _SubscriptionRequest.newBuilder()
            .setCacheName(options.getCacheName())
            .setTopic(options.getTopicName())
            .setResumeAtTopicSequenceNumber(
                options.subscriptionState.getResumeAtTopicSequenceNumber())
            .setSequencePage(options.subscriptionState.getResumeAtTopicSequencePage())
            .build();

    try {
      connection.subscribe(subscriptionRequest, subscription);
      options.subscriptionState.setSubscribed();
    } catch (Exception e) {
      future.completeExceptionally(
          new TopicSubscribeResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  private void scheduleRetry(Runnable retryAction) {
    scheduler.schedule(retryAction, 5, TimeUnit.SECONDS);
  }

  private void handleSubscriptionCompleted() {
    this.options.onCompleted();
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
    logger.debug(
        "discontinuity {}, {}, {}, {}, {}, {}",
        options.getCacheName(),
        options.getTopicName(),
        discontinuityItem.getDiscontinuity().getLastTopicSequence(),
        discontinuityItem.getDiscontinuity().getNewTopicSequence(),
        discontinuityItem.getDiscontinuity().getLastTopicSequence(),
        discontinuityItem.getDiscontinuity().getNewTopicSequence());

    options.onDiscontinuity(
        new TopicDiscontinuity(
            (int) discontinuityItem.getDiscontinuity().getLastTopicSequence(),
            (int) discontinuityItem.getDiscontinuity().getNewTopicSequence(),
            (int) discontinuityItem.getDiscontinuity().getNewSequencePage()));
  }

  private void handleSubscriptionHeartbeat() {
    logger.debug("heartbeat {} {}", options.getCacheName(), options.getTopicName());
    options.onHeartbeat();
  }

  private void handleSubscriptionUnknown() {
    logger.warn("unknown {} {}", options.getCacheName(), options.getTopicName());
  }

  private void handleSubscriptionItemMessage(_SubscriptionItem item) {
    _TopicItem topicItem = item.getItem();
    _TopicValue topicValue = topicItem.getValue();
    options.subscriptionState.setResumeAtTopicSequenceNumber(
        (int) topicItem.getTopicSequenceNumber());
    options.subscriptionState.setResumeAtTopicSequencePage((int) topicItem.getSequencePage());

    logger.debug(
        "Received Item on subscription stream: {}, {}, {}, {}, {}",
        options.getCacheName(),
        options.getTopicName(),
        topicItem.getPublisherId(),
        topicItem.getTopicSequenceNumber(),
        topicItem.getSequencePage());
    TopicMessage message;

    switch (topicValue.getKindCase()) {
      case TEXT:
        message =
            handleSubscriptionTextMessage(topicValue.getText(), item.getItem().getPublisherId());
        this.options.onItem(message);
        break;
      case BINARY:
        message =
            handleSubscriptionBinaryMessage(
                topicValue.getBinary().toByteArray(), item.getItem().getPublisherId());
        this.options.onItem(message);
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
    subscription.cancel(
        "Unsubscribing from topic: "
            + options.getTopicName()
            + " in cache: "
            + options.getCacheName(),
        null);
  }

  @Override
  public void close() {
    if (subscription != null) {
      subscription.onCompleted();
    }
    scheduler.shutdown();
  }
}
