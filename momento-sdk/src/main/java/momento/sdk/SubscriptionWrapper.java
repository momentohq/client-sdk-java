package momento.sdk;

import com.google.protobuf.ByteString;
import grpc.cache_client.pubsub._SubscriptionItem;
import grpc.cache_client.pubsub._SubscriptionRequest;
import grpc.cache_client.pubsub._TopicValue;
import io.grpc.stub.StreamObserver;
import java.io.Closeable;
import java.util.concurrent.CompletableFuture;
import momento.sdk.exceptions.CacheServiceExceptionMapper;
import momento.sdk.exceptions.InternalServerException;
import momento.sdk.responses.topic.SubscriptionState;
import momento.sdk.responses.topic.TopicMessage;
import momento.sdk.responses.topic.TopicSubscribeResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SubscriptionWrapper implements Closeable {
  private final Logger logger = LoggerFactory.getLogger(SubscriptionWrapper.class);
  private final ScsTopicGrpcStubsManager grpcManager;
  private final String cacheName;
  private final String topicName;
  private final SubscriptionState subscriptionState;
  private final ISubscriptionCallbacks options;
  private StreamObserver<_SubscriptionItem> subscription;

  public SubscriptionWrapper(
      ScsTopicGrpcStubsManager grpcManager,
      String cacheName,
      String topicName,
      SubscriptionState subscriptionState,
      ISubscriptionCallbacks options) {
    this.grpcManager = grpcManager;
    this.cacheName = cacheName;
    this.topicName = topicName;
    this.subscriptionState = subscriptionState;
    this.options = options;
  }

  public CompletableFuture<Void> subscribe() {
    CompletableFuture<Void> future = new CompletableFuture<>();

    subscription =
        new StreamObserver<_SubscriptionItem>() {
          boolean firstMessage = true;

          @Override
          public void onNext(_SubscriptionItem item) {
            if (firstMessage) {
              if (item.getKindCase() != _SubscriptionItem.KindCase.HEARTBEAT) {
                future.completeExceptionally(
                    new InternalServerException(
                        "Expected heartbeat message for topic "
                            + topicName
                            + " on cache "
                            + cacheName
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
            if (firstMessage) {
              firstMessage = false;
              future.completeExceptionally(t);
            } else {
              handleSubscriptionError(t);
            }
          }

          @Override
          public void onCompleted() {
            handleSubscriptionCompleted();
          }
        };

    _SubscriptionRequest subscriptionRequest =
        _SubscriptionRequest.newBuilder()
            .setCacheName(cacheName)
            .setTopic(topicName)
            .setResumeAtTopicSequenceNumber(subscriptionState.getResumeAtTopicSequenceNumber())
            .build();

    try {
      grpcManager.getStub().subscribe(subscriptionRequest, subscription);
      subscriptionState.setSubscribed();
    } catch (Exception e) {
      future.completeExceptionally(
          new TopicSubscribeResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
    return future;
  }

  private void handleSubscriptionError(Throwable t) {
    logger.trace("error " + cacheName + " " + topicName + " " + t.getMessage());
    this.options.onError(t);
  }

  private void handleSubscriptionCompleted() {
    logger.trace("completed " + cacheName + " " + topicName);
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
        cacheName,
        topicName,
        discontinuityItem.getDiscontinuity().getLastTopicSequence(),
        discontinuityItem.getDiscontinuity().getNewTopicSequence());
  }

  private void handleSubscriptionHeartbeat() {
    logger.debug("heartbeat " + " " + cacheName + " " + topicName);
  }

  private void handleSubscriptionUnknown() {
    logger.warn("unknown " + cacheName + " " + topicName);
  }

  private void handleSubscriptionItemMessage(_SubscriptionItem item) {
    _TopicValue topicValue = item.getItem().getValue();
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
    this.close();
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
