package momento.sdk;

import grpc.cache_client.pubsub.PubsubGrpc;
import grpc.cache_client.pubsub.PubsubGrpc.PubsubStub;
import io.grpc.ManagedChannel;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import momento.sdk.auth.CredentialProvider;
import momento.sdk.config.TopicConfiguration;

class StaticUnaryGrpcConnectionPool implements UnaryTopicGrpcConnectionPool {
  private final Duration deadline;
  private final AtomicInteger index = new AtomicInteger(0);
  private final int numUnaryGrpcChannels;
  private final List<ManagedChannel> unaryChannels;
  private final List<PubsubGrpc.PubsubStub> unaryStubs;

  public StaticUnaryGrpcConnectionPool(
      CredentialProvider credentialProvider,
      TopicConfiguration configuration,
      UUID connectionIdKey) {
    this.deadline = configuration.getTransportStrategy().getGrpcConfiguration().getDeadline();
    this.numUnaryGrpcChannels =
        configuration.getTransportStrategy().getGrpcConfiguration().getNumUnaryGrpcChannels();
    this.unaryChannels =
        IntStream.range(0, this.numUnaryGrpcChannels)
            .mapToObj(
                i ->
                    TopicGrpcConnectionPoolUtils.setupConnection(
                        credentialProvider, configuration, connectionIdKey))
            .collect(Collectors.toList());
    this.unaryStubs = unaryChannels.stream().map(PubsubGrpc::newStub).collect(Collectors.toList());
  }

  @Override
  public PubsubStub getNextUnaryStub() {
    return unaryStubs
        .get(this.index.getAndIncrement() % this.numUnaryGrpcChannels)
        .withDeadlineAfter(deadline.toMillis(), TimeUnit.MILLISECONDS);
  }

  @Override
  public void close() {
    unaryChannels.forEach(ManagedChannel::shutdown);
  }
}
