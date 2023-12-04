package momento.sdk;

import grpc.cache_client.pubsub._SubscriptionRequest;
import io.grpc.stub.StreamObserver;
import momento.sdk.exceptions.CacheServiceExceptionMapper;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;

public class SubscriptionWrapper implements Closeable {

    private final ScsTopicGrpcStubsManager grpcManager;
    private final String cacheName;
    private final String topicName;

    private Long lastSequenceNumber;
    private boolean subscribed;

    public SubscriptionWrapper(ScsTopicGrpcStubsManager grpcManager, String cacheName,
                               String topicName) {
        this.grpcManager = grpcManager;
        this.cacheName = cacheName;
        this.topicName = topicName;
    }

    public CompletableFuture<Void> subscribe() {
        CompletableFuture<Void> future = new CompletableFuture<>();

        _SubscriptionRequest.Builder requestBuilder = _SubscriptionRequest.newBuilder()
                .setCacheName(cacheName)
                .setTopic(topicName);

        if (lastSequenceNumber != null) {
            requestBuilder.setResumeAtTopicSequenceNumber(lastSequenceNumber);
        }

        _SubscriptionRequest request = requestBuilder.build();

        try {
            grpcManager.getStub().subscribe(request, new StreamObserver() {

                @Override
                public void onNext(Object value) {

                }

                @Override
                public void onError(Throwable t) {
                    // Handle error
                    future.completeExceptionally(CacheServiceExceptionMapper.convert(t));
                }

                @Override
                public void onCompleted() {
                    // Handle completion
                    future.complete(null);
                }
            });
        } catch (Exception e) {
            // Exception during gRPC call setup
            future.completeExceptionally(CacheServiceExceptionMapper.convert(e));
        }

        return future;
    }

    @Override
    public void close() throws IOException {

    }

    // Implement the rest of the functionality including the GetNextRelevantMessageFromGrpcStreamAsync method.

}
