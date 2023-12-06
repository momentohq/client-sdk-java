package momento.sdk;

import com.google.protobuf.ByteString;
import grpc.cache_client.pubsub._PublishRequest;
import grpc.cache_client.pubsub._TopicValue;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import momento.sdk.auth.CredentialProvider;
import momento.sdk.config.Configuration;
import momento.sdk.exceptions.CacheServiceExceptionMapper;
import momento.sdk.responses.topic.SubscriptionState;
import momento.sdk.responses.topic.TopicPublishResponse;
import momento.sdk.responses.topic.TopicSubscribeResponse;

public class ScsTopicClient extends ScsClient {

  private final ScsTopicGrpcStubsManager topicGrpcStubsManager;
  private final CredentialProvider credentialProvider;

  public ScsTopicClient(
      @Nonnull CredentialProvider credentialProvider, @Nonnull Configuration configuration) {
    this.credentialProvider = credentialProvider;
    this.topicGrpcStubsManager = new ScsTopicGrpcStubsManager(credentialProvider, configuration);
  }

  public CompletableFuture<TopicPublishResponse> publish(
      String cacheName, String topicName, byte[] value) {
    _TopicValue topicValue = _TopicValue.newBuilder().setBinary(ByteString.copyFrom(value)).build();
    return sendPublish(cacheName, topicName, topicValue);
  }

  public CompletableFuture<TopicPublishResponse> publish(
      String cacheName, String topicName, String value) {
    _TopicValue topicValue = _TopicValue.newBuilder().setText(value).build();
    return sendPublish(cacheName, topicName, topicValue);
  }

  public CompletableFuture<TopicSubscribeResponse> subscribe(
      String cacheName, String topicName, ISubscribeCallOptions options) {
    return sendSubscribe(cacheName, topicName, options);
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
          .publish(
              request,
              new StreamObserver() {

                @Override
                public void onNext(Object value) {
                  // Success
                }

                @Override
                public void onError(Throwable t) {
                  // Error
                  future.completeExceptionally(CacheServiceExceptionMapper.convert(t));
                }

                @Override
                public void onCompleted() {
                  // Completed
                  future.complete(new TopicPublishResponse.Success());
                }
              });
    } catch (Exception e) {
      // Exception during gRPC call setup
      future.completeExceptionally(CacheServiceExceptionMapper.convert(e));
    }

    return future;
  }

  private CompletableFuture<TopicSubscribeResponse> sendSubscribe(
      String cacheName, String topicName, ISubscribeCallOptions options) {
    try {
      ValidationUtils.checkCacheNameValid(cacheName);
      ValidationUtils.checkTopicNameValid(topicName);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new TopicSubscribeResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }

    SubscriptionWrapper subscriptionWrapper;
    SubscriptionState subState = new SubscriptionState();
    subscriptionWrapper =
        new SubscriptionWrapper(topicGrpcStubsManager, cacheName, topicName, subState, options);
    final CompletableFuture<Void> subscribeFuture = subscriptionWrapper.subscribe();
    return subscribeFuture.handle(
        (v, ex) -> {
          if (ex != null) {
            return new TopicSubscribeResponse.Error(CacheServiceExceptionMapper.convert(ex));
          } else {
            subState.setUnsubscribeFn(subscriptionWrapper::unsubscribe);
            return new TopicSubscribeResponse.Subscription(subState);
          }
        });
  }

  @Override
  public void close() {
    topicGrpcStubsManager.close();
  }
}
