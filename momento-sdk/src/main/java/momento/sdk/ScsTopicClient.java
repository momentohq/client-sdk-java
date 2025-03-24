package momento.sdk;

import com.google.protobuf.ByteString;
import grpc.cache_client.pubsub._PublishRequest;
import grpc.cache_client.pubsub._SubscriptionItem;
import grpc.cache_client.pubsub._SubscriptionRequest;
import grpc.cache_client.pubsub._TopicValue;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import momento.sdk.auth.CredentialProvider;
import momento.sdk.config.TopicConfiguration;
import momento.sdk.exceptions.CacheServiceExceptionMapper;
import momento.sdk.internal.SubscriptionState;
import momento.sdk.responses.topic.TopicPublishResponse;
import momento.sdk.responses.topic.TopicSubscribeResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScsTopicClient extends ScsClientBase {

  private final Logger logger = LoggerFactory.getLogger(ScsTopicClient.class);
  private final ScsTopicGrpcStubsManager topicGrpcStubsManager;
  private final long DEFAULT_REQUEST_TIMEOUT_SECONDS = 5;

  public ScsTopicClient(
      @Nonnull CredentialProvider credentialProvider, @Nonnull TopicConfiguration configuration) {
    super(null);
    this.topicGrpcStubsManager = new ScsTopicGrpcStubsManager(credentialProvider, configuration);
  }

  public CompletableFuture<TopicPublishResponse> publish(
      String cacheName, String topicName, byte[] value) {
    try {
      ValidationUtils.checkCacheNameValid(cacheName);
      ValidationUtils.checkTopicNameValid(topicName);
      ValidationUtils.ensureValidValue(value);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new TopicPublishResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }

    _TopicValue topicValue = _TopicValue.newBuilder().setBinary(ByteString.copyFrom(value)).build();
    return sendPublish(cacheName, topicName, topicValue);
  }

  public CompletableFuture<TopicPublishResponse> publish(
      String cacheName, String topicName, String value) {
    try {
      ValidationUtils.checkCacheNameValid(cacheName);
      ValidationUtils.checkTopicNameValid(topicName);
      ValidationUtils.ensureValidValue(value);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new TopicPublishResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }

    _TopicValue topicValue = _TopicValue.newBuilder().setText(value).build();
    return sendPublish(cacheName, topicName, topicValue);
  }

  public CompletableFuture<TopicSubscribeResponse> subscribe(
      String cacheName, String topicName, ISubscriptionCallbacks options) {
    try {
      ValidationUtils.checkCacheNameValid(cacheName);
      ValidationUtils.checkTopicNameValid(topicName);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new TopicSubscribeResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }

    SubscriptionState subscriptionState = new SubscriptionState();
    TopicSubscribeResponse.Subscription subscription =
        new TopicSubscribeResponse.Subscription(subscriptionState);
    SendSubscribeOptions sendSubscribeOptions =
        new SendSubscribeOptions(
            cacheName,
            topicName,
            options::onItem,
            options::onCompleted,
            options::onError,
            options::onDiscontinuity,
            options::onHeartbeat,
            options::onConnectionLost,
            options::onConnectionRestored,
            subscriptionState,
            subscription);

    return sendSubscribe(sendSubscribeOptions);
  }

  private CompletableFuture<TopicPublishResponse> sendPublish(
      String cacheName, String topicName, _TopicValue value) {
    CompletableFuture<TopicPublishResponse> future = new CompletableFuture<>();

    _PublishRequest request =
        _PublishRequest.newBuilder()
            .setCacheName(cacheName)
            .setTopic(topicName)
            .setValue(value)
            .build();

    try {
      topicGrpcStubsManager
          .getStub()
          .withDeadlineAfter(
              topicGrpcStubsManager
                  .getConfiguration()
                  .getTransportStrategy()
                  .getGrpcConfiguration()
                  .getDeadline()
                  .getSeconds(),
              TimeUnit.SECONDS)
          .publish(
              request,
              new StreamObserver() {

                @Override
                public void onNext(Object value) {
                  // Do nothing
                }

                @Override
                public void onError(Throwable t) {
                  future.complete(
                      new TopicPublishResponse.Error(CacheServiceExceptionMapper.convert(t)));
                }

                @Override
                public void onCompleted() {
                  future.complete(new TopicPublishResponse.Success());
                }
              });
    } catch (Exception e) {
      // Exception during gRPC call setup
      future.completeExceptionally(
          new TopicPublishResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }

    return future;
  }

  private CompletableFuture<TopicSubscribeResponse> sendSubscribe(
      SendSubscribeOptions sendSubscribeOptions) {
    SubscriptionWrapper subscriptionWrapper;

    IScsTopicConnection connection =
        new IScsTopicConnection() {
          @Override
          public void close() {
            logger.warn("Closing the connection (for testing purposes only)");
          }

          @Override
          public void open() {
            logger.warn("Opening the connection (for testing purposes only)");
          }

          @Override
          public void subscribe(
              _SubscriptionRequest subscriptionRequest,
              CancelableClientCallStreamObserver<_SubscriptionItem> subscription) {
            topicGrpcStubsManager.getStub().subscribe(subscriptionRequest, subscription);
          }
        };

    long configuredTimeoutSeconds =
        topicGrpcStubsManager
            .getConfiguration()
            .getTransportStrategy()
            .getGrpcConfiguration()
            .getDeadline()
            .getSeconds();
    long firstMessageSubscribeTimeoutSeconds =
        configuredTimeoutSeconds > 0 ? configuredTimeoutSeconds : DEFAULT_REQUEST_TIMEOUT_SECONDS;

    subscriptionWrapper =
        new SubscriptionWrapper(
            connection, sendSubscribeOptions, firstMessageSubscribeTimeoutSeconds);
    final CompletableFuture<Void> subscribeFuture = subscriptionWrapper.subscribeWithRetry();
    return subscribeFuture.handle(
        (v, ex) -> {
          if (ex != null) {
            return new TopicSubscribeResponse.Error(CacheServiceExceptionMapper.convert(ex));
          } else {
            sendSubscribeOptions.subscriptionState.setUnsubscribeFn(
                subscriptionWrapper::unsubscribe);
            return new TopicSubscribeResponse.Subscription(sendSubscribeOptions.subscriptionState);
          }
        });
  }

  @Override
  public void doClose() {
    topicGrpcStubsManager.close();
  }
}
