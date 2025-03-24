package momento.sdk;

import com.google.protobuf.ByteString;
import grpc.cache_client.pubsub._PublishRequest;
import grpc.cache_client.pubsub._TopicValue;
import grpc.common._Empty;
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
import momento.sdk.retry.SubscriptionRetryStrategy;

public class ScsTopicClient extends ScsClientBase {

  private final ScsTopicGrpcStubsManager topicGrpcStubsManager;
  private final SubscriptionRetryStrategy subscriptionRetryStrategy;

  public ScsTopicClient(
      @Nonnull CredentialProvider credentialProvider, @Nonnull TopicConfiguration configuration) {
    super(null);
    this.topicGrpcStubsManager = new ScsTopicGrpcStubsManager(credentialProvider, configuration);
    this.subscriptionRetryStrategy = configuration.getSubscriptionRetryStrategy();
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
      String cacheName, String topicName, ISubscriptionCallbacks callbacks) {
    try {
      ValidationUtils.checkCacheNameValid(cacheName);
      ValidationUtils.checkTopicNameValid(topicName);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new TopicSubscribeResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }

    return sendSubscribe(cacheName, topicName, callbacks);
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
              new StreamObserver<_Empty>() {

                @Override
                public void onNext(_Empty value) {
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
      String cacheName, String topicName, ISubscriptionCallbacks callbacks) {
    final SubscriptionState subscriptionState = new SubscriptionState();

    final IScsTopicConnection connection =
        (request, subscription) -> topicGrpcStubsManager.getStub().subscribe(request, subscription);

    @SuppressWarnings("resource") // the wrapper closes itself when a subscription ends.
    final SubscriptionWrapper subscriptionWrapper =
        new SubscriptionWrapper(
            cacheName,
            topicName,
            connection,
            callbacks,
            subscriptionState,
            subscriptionRetryStrategy);
    final CompletableFuture<Void> subscribeFuture = subscriptionWrapper.subscribeWithRetry();
    return subscribeFuture.handle(
        (v, ex) -> {
          if (ex != null) {
            return new TopicSubscribeResponse.Error(CacheServiceExceptionMapper.convert(ex));
          } else {
            subscriptionState.setUnsubscribeFn(subscriptionWrapper::unsubscribe);
            return new TopicSubscribeResponse.Subscription(subscriptionState);
          }
        });
  }

  @Override
  public void doClose() {
    topicGrpcStubsManager.close();
  }
}
