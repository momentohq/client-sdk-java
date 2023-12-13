package momento.sdk;

import com.google.protobuf.ByteString;
import grpc.cache_client.pubsub._SubscriptionItem;
import grpc.cache_client.pubsub._SubscriptionRequest;
import grpc.cache_client.pubsub._TopicItem;
import grpc.cache_client.pubsub._TopicValue;
import io.grpc.Status;
import java.io.Closeable;
import java.util.concurrent.CompletableFuture;
import momento.sdk.exceptions.CacheServiceExceptionMapper;
import momento.sdk.exceptions.InternalServerException;
import momento.sdk.responses.topic.TopicMessage;
import momento.sdk.responses.topic.TopicSubscribeResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SubscriptionWrapper implements Closeable {
  private final Logger logger = LoggerFactory.getLogger(SubscriptionWrapper.class);
  private final ScsTopicGrpcStubsManager grpcManager;
  private final SendSubscribeOptions options;

  // TODO: make this private again

  // public StreamObserver<_SubscriptionItem> subscription;
  public CancelableClientCallStreamObserver<_SubscriptionItem> subscription;

  public SubscriptionWrapper(ScsTopicGrpcStubsManager grpcManager, SendSubscribeOptions options) {
    this.grpcManager = grpcManager;
    this.options = options;
  }

  public CompletableFuture<Void> subscribe() {
    logger.info("Subscribing " + options.getCacheName() + " " + options.getTopicName());
    CompletableFuture<Void> future = new CompletableFuture<>();
    subscription =
        new CancelableClientCallStreamObserver<_SubscriptionItem>() {
          boolean firstMessage = true;

          @Override
          public boolean isReady() {
            return false;
          }

          @Override
          public void setOnReadyHandler(Runnable onReadyHandler) {}

          @Override
          public void request(int count) {}

          @Override
          public void setMessageCompression(boolean enable) {}

          @Override
          public void disableAutoInboundFlowControl() {}

          @Override
          public void onNext(_SubscriptionItem item) {
            logger.info("onNext callback Invoked");
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
            handleSubscriptionItem(item);
          }

          @Override
          public void onError(Throwable t) {
            logger.info("onError callback Invoked");
            if (firstMessage) {
              logger.info("onError callback inside if block Invoked");
              firstMessage = false;
              future.completeExceptionally(t);

            } else {
              logger.info("onError callback inside else block Invoked" + t.getMessage());
              handleSubscriptionError(t);
            }
          }

          @Override
          public void onCompleted() {
            logger.info("onCompleted callback Invoked");
            handleSubscriptionCompleted();
          }
        };

    _SubscriptionRequest subscriptionRequest =
        _SubscriptionRequest.newBuilder()
            .setCacheName(options.getCacheName())
            .setTopic(options.getTopicName())
            .setResumeAtTopicSequenceNumber(
                options.subscriptionState.getResumeAtTopicSequenceNumber())
            .build();

    try {
      grpcManager.getStub().subscribe(subscriptionRequest, subscription);
      logger.info("subscribed " + options.getCacheName() + " " + options.getTopicName());
      options.subscriptionState.setSubscribed();
    } catch (Exception e) {
      future.completeExceptionally(
          new TopicSubscribeResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
    return future;
  }

  private void handleSubscriptionError(Throwable t) {
    logger.info(
        "error " + options.getCacheName() + " " + options.getTopicName() + " " + t.getMessage());
    if (t instanceof io.grpc.StatusRuntimeException) {
      io.grpc.StatusRuntimeException statusRuntimeException = (io.grpc.StatusRuntimeException) t;
      if (statusRuntimeException.getStatus().getCode() == Status.Code.UNAVAILABLE) {
        logger.info("WE GOT AN UNAVAILABLE ERROR");
        unsubscribe();
        this.subscribe();
      }
    } else {
      logger.info("WE GOT AN ERROR" + t.getMessage());
      this.options.onError(t);
    }
  }

  private void handleSubscriptionCompleted() {
    logger.trace("completed " + options.getCacheName() + " " + options.getTopicName());
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
        "{}, {}, {}, {}",
        options.getCacheName(),
        options.getTopicName(),
        discontinuityItem.getDiscontinuity().getLastTopicSequence(),
        discontinuityItem.getDiscontinuity().getNewTopicSequence());
  }

  private void handleSubscriptionHeartbeat() {
    logger.debug("heartbeat {} {}", options.getCacheName(), options.getTopicName());
  }

  private void handleSubscriptionUnknown() {
    logger.warn("unknown {} {}", options.getCacheName(), options.getTopicName());
  }

  private void handleSubscriptionItemMessage(_SubscriptionItem item) {
    _TopicItem topicItem = item.getItem();
    _TopicValue topicValue = topicItem.getValue();
    options.subscriptionState.setResumeAtTopicSequenceNumber(
        (int) topicItem.getTopicSequenceNumber());
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
    //    this.close();
    logger.info("Unsubscribing " + options.getCacheName() + " " + options.getTopicName());
    subscription.cancel(null, null);
  }

  @Override
  public void close() {
    if (subscription != null) {
      subscription.onCompleted();
    }
    if (grpcManager != null) {
      grpcManager.close();
    }
  }
}
