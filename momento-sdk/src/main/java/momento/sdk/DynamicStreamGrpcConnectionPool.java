package momento.sdk;

import grpc.cache_client.pubsub.PubsubGrpc;
import io.grpc.ManagedChannel;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import momento.sdk.auth.CredentialProvider;
import momento.sdk.config.TopicConfiguration;
import momento.sdk.exceptions.ClientSdkException;
import momento.sdk.exceptions.MomentoErrorCode;
import momento.sdk.internal.GrpcChannelOptions;

public class DynamicStreamGrpcConnectionPool implements StreamTopicGrpcConnectionPool {
  private final CredentialProvider credentialProvider;
  private final TopicConfiguration configuration;
  private final UUID connectionIdKey;

  private final AtomicInteger index = new AtomicInteger(0);
  private final AtomicInteger currentNumStreamGrpcChannels = new AtomicInteger(1);
  private final int maxStreamGrpcChannels;

  private final int currentMaxConcurrentStreams;
  private final AtomicInteger currentNumActiveStreams = new AtomicInteger(0);

  private final CopyOnWriteArrayList<ManagedChannel> streamChannels;
  private final CopyOnWriteArrayList<StreamStubWithCount> streamStubs;

  public DynamicStreamGrpcConnectionPool(
      CredentialProvider credentialProvider,
      TopicConfiguration configuration,
      UUID connectionIdKey) {
    this.currentMaxConcurrentStreams = GrpcChannelOptions.NUM_CONCURRENT_STREAMS_PER_GRPC_CHANNEL;
    this.maxStreamGrpcChannels =
        configuration.getTransportStrategy().getGrpcConfiguration().getNumStreamGrpcChannels();

    this.credentialProvider = credentialProvider;
    this.configuration = configuration;
    this.connectionIdKey = connectionIdKey;

    this.streamChannels =
        IntStream.range(0, this.currentNumStreamGrpcChannels.get())
            .mapToObj(
                i ->
                    TopicGrpcConnectionPoolUtils.setupConnection(
                        credentialProvider, configuration, connectionIdKey))
            .collect(Collectors.toCollection(CopyOnWriteArrayList::new));
    this.streamStubs =
        streamChannels.stream()
            .map(PubsubGrpc::newStub)
            .map(StreamStubWithCount::new)
            .collect(Collectors.toCollection(CopyOnWriteArrayList::new));
  }

  // Multiple threads could get to the point of seeing currentNumActiveStreams ==
  // currentMaxConcurrentStreams,
  // but we need to ensure only one thread will add a new channel at a time so that we don't exceed
  // the max number of channels.
  private void addNewChannel() {
    final int updatedCount = this.currentNumStreamGrpcChannels.incrementAndGet();

    if (updatedCount > this.maxStreamGrpcChannels) {
      this.currentNumStreamGrpcChannels.decrementAndGet();
      return;
    }

    this.streamChannels.add(
        TopicGrpcConnectionPoolUtils.setupConnection(
            credentialProvider, configuration, connectionIdKey));
    this.streamStubs.add(
        new StreamStubWithCount(
            PubsubGrpc.newStub(
                TopicGrpcConnectionPoolUtils.setupConnection(
                    credentialProvider, configuration, connectionIdKey))));
  }

  @Override
  public StreamStubWithCount getNextStreamStub() {
    // Check if we've reached the current max number of active streams.
    if (this.currentNumActiveStreams.get() == this.currentMaxConcurrentStreams) {
      // If we have not yet reached the maximum number of channels, add a new channel.
      if (this.currentNumStreamGrpcChannels.get() < this.maxStreamGrpcChannels) {
        this.addNewChannel();
      } else {
        // Otherwise return an error because all channels and streams are occupied.
        throw new ClientSdkException(
            MomentoErrorCode.CLIENT_RESOURCE_EXHAUSTED,
            "Maximum number of active subscriptions reached");
      }
    }

    // Try to get a client with capacity for another subscription
    // by round-robining through the stubs.
    // Allow up to maximumActiveSubscriptions attempts to account for large bursts of requests.
    final int maximumActiveSubscriptions =
        this.currentNumStreamGrpcChannels.get()
            * GrpcChannelOptions.NUM_CONCURRENT_STREAMS_PER_GRPC_CHANNEL;
    for (int i = 0; i < maximumActiveSubscriptions; i++) {
      final StreamStubWithCount stubWithCount =
          streamStubs.get(index.getAndIncrement() % this.currentNumStreamGrpcChannels.get());
      try {
        stubWithCount.acquireStubOrThrow();
        this.currentNumActiveStreams.incrementAndGet();
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
