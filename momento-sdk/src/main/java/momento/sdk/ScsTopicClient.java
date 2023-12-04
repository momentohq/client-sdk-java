package momento.sdk;

import com.google.protobuf.ByteString;
import grpc.cache_client.pubsub._PublishRequest;
import grpc.cache_client.pubsub._SubscriptionRequest;
import grpc.cache_client.pubsub._TopicValue;
import io.grpc.stub.StreamObserver;
import momento.sdk.auth.CredentialProvider;
import momento.sdk.config.Configuration;
import momento.sdk.exceptions.CacheServiceExceptionMapper;
import momento.sdk.responses.topic.TopicPublishResponse;
import momento.sdk.responses.topic.TopicSubscribeResponse;

import javax.annotation.Nonnull;
import java.util.concurrent.CompletableFuture;

public class ScsTopicClient extends ScsClient {

    private final ScsTopicGrpcStubsManager topicGrpcStubsManager;
    private final CredentialProvider credentialProvider;

    public ScsTopicClient(
            @Nonnull CredentialProvider credentialProvider, @Nonnull Configuration configuration) {
        this.credentialProvider = credentialProvider;
        this.topicGrpcStubsManager = new ScsTopicGrpcStubsManager(credentialProvider, configuration);
    }

    public CompletableFuture<TopicPublishResponse> publish(String cacheName, String topicName, byte[] value) {
        _TopicValue topicValue = _TopicValue.newBuilder().setBinary(ByteString.copyFrom(value)).build();
        return sendPublish(cacheName, topicName, topicValue);
    }

    public CompletableFuture<TopicPublishResponse> publish(String cacheName, String topicName, String value) {
        _TopicValue topicValue = _TopicValue.newBuilder().setText(value).build();
        return sendPublish(cacheName, topicName, topicValue);
    }

    public CompletableFuture<TopicSubscribeResponse> subscribe(String cacheName, String topicName, Long resumeAtTopicSequenceNumber) {
        return sendSubscribe(cacheName, topicName, resumeAtTopicSequenceNumber);
    }

    public CompletableFuture<TopicSubscribeResponse> subscribe(String cacheName, String topicName) {
        return sendSubscribe(cacheName, topicName, null);
    }

    private CompletableFuture<TopicPublishResponse> sendPublish(String cacheName, String topicName, _TopicValue value) {
        CompletableFuture<TopicPublishResponse> future = new CompletableFuture<>();

        _PublishRequest request = _PublishRequest.newBuilder()
                .setCacheName(cacheName)
                .setTopic(topicName)
                .setValue(value)
                .build();

        try {
            topicGrpcStubsManager.getStub().publish(request, new StreamObserver() {

                @Override
                public void onNext(Object value) {
                    // Success
                    future.complete(new TopicPublishResponse.Success());
                }

                @Override
                public void onError(Throwable t) {
                    // Error
                    future.completeExceptionally(CacheServiceExceptionMapper.convert(t));
                }

                @Override
                public void onCompleted() {
                    // Completed
                }
            });
        } catch (Exception e) {
            // Exception during gRPC call setup
            future.completeExceptionally(CacheServiceExceptionMapper.convert(e));
        }

        return future;
    }

    private CompletableFuture<TopicSubscribeResponse> sendSubscribe(String cacheName, String topicName, Long resumeAtTopicSequenceNumber) {
        CompletableFuture<TopicSubscribeResponse> future = new CompletableFuture<>();

        _SubscriptionRequest.Builder requestBuilder = _SubscriptionRequest.newBuilder()
                .setCacheName(cacheName)
                .setTopic(topicName);

        if (resumeAtTopicSequenceNumber != null) {
            requestBuilder.setResumeAtTopicSequenceNumber(resumeAtTopicSequenceNumber);
        }

        _SubscriptionRequest request = requestBuilder.build();

        SubscriptionWrapper subscriptionWrapper;
        try {
            subscriptionWrapper = new SubscriptionWrapper(topicGrpcStubsManager, cacheName, topicName);
            subscriptionWrapper.subscribe().join();
        } catch (Exception e) {
            future.completeExceptionally(CacheServiceExceptionMapper.convert(e));
        }

        return future;
    }


    @Override
    public void close() {
        topicGrpcStubsManager.close();
    }
}
