package momento.sdk;

import grpc.cache_client.pubsub.PubsubGrpc;
import io.grpc.ManagedChannel;
import java.io.Closeable;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import momento.sdk.auth.CredentialProvider;
import momento.sdk.config.TopicConfiguration;
import momento.sdk.exceptions.ClientSdkException;
import momento.sdk.exceptions.MomentoErrorCode;
import momento.sdk.internal.GrpcChannelOptions;

class StaticStreamGrpcConnectionPool implements StreamTopicGrpcConnectionPool, Closeable {
  private final AtomicInteger index = new AtomicInteger(0);
  private final int numStreamGrpcChannels;
  private final List<ManagedChannel> streamChannels;
  private final List<StreamStubWithCount> streamStubs;

  public StaticStreamGrpcConnectionPool(
      CredentialProvider credentialProvider,
      TopicConfiguration configuration,
      UUID connectionIdKey) {
    this.numStreamGrpcChannels =
        configuration.getTransportStrategy().getGrpcConfiguration().getNumStreamGrpcChannels();
    this.streamChannels =
        IntStream.range(0, this.numStreamGrpcChannels)
            .mapToObj(
                i ->
                    TopicGrpcConnectionPoolUtils.setupConnection(
                        credentialProvider, configuration, connectionIdKey))
            .collect(Collectors.toList());
    this.streamStubs =
        streamChannels.stream()
            .map(PubsubGrpc::newStub)
            .map(StreamStubWithCount::new)
            .collect(Collectors.toList());
  }

  @Override
  public StreamStubWithCount getNextStreamStub() {
    // Try to get a client with capacity for another subscription
    // by round-robining through the stubs.
    // Allow up to maximumActiveSubscriptions attempts to account for large bursts of requests.
    final int maximumActiveSubscriptions =
        this.numStreamGrpcChannels * GrpcChannelOptions.NUM_CONCURRENT_STREAMS_PER_GRPC_CHANNEL;
    for (int i = 0; i < maximumActiveSubscriptions; i++) {
      final StreamStubWithCount stubWithCount =
          streamStubs.get(index.getAndIncrement() % this.numStreamGrpcChannels);
      try {
        stubWithCount.acquireStubOrThrow();
        return stubWithCount;
      } catch (ClientSdkException e) {
        // If the stub is at capacity, continue to the next one.
        continue;
      }
    }

    // Otherwise return an error if no stubs have capacity.
    throw new ClientSdkException(
        MomentoErrorCode.CLIENT_RESOURCE_EXHAUSTED,
        "Maximum number of active subscriptions reached");
  }

  @Override
  public void close() {
    streamChannels.forEach(ManagedChannel::shutdown);
  }
}
