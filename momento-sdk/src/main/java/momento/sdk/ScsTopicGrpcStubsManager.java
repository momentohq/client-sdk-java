package momento.sdk;

import grpc.cache_client.pubsub.PubsubGrpc;
import io.grpc.ClientInterceptor;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.io.Closeable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nonnull;
import momento.sdk.auth.CredentialProvider;
import momento.sdk.config.TopicConfiguration;
import momento.sdk.config.middleware.Middleware;
import momento.sdk.config.middleware.MiddlewareRequestHandlerContext;
import momento.sdk.internal.GrpcChannelOptions;

/**
 * Manager responsible for GRPC channels and stubs for the Topics.
 *
 * <p>The business layer, will get request stubs from this layer. This keeps the two layers
 * independent and any future pooling of channels can happen exclusively in the manager without
 * impacting the API business logic.
 */
final class ScsTopicGrpcStubsManager implements Closeable {

  private final List<ManagedChannel> unaryChannels;
  private final List<PubsubGrpc.PubsubStub> unaryStubs;
  private final AtomicInteger unaryIndex = new AtomicInteger(0);

  private final List<ManagedChannel> streamChannels;
  private final List<PubsubGrpc.PubsubStub> streamStubs;
  private final AtomicInteger streamIndex = new AtomicInteger(0);

  public static final UUID CONNECTION_ID_KEY = UUID.randomUUID();

  private final int numUnaryGrpcChannels;
  private final int numStreamGrpcChannels;
  private final TopicConfiguration configuration;
  private final Duration deadline;

  ScsTopicGrpcStubsManager(
      @Nonnull CredentialProvider credentialProvider, @Nonnull TopicConfiguration configuration) {
    this.configuration = configuration;
    this.deadline = configuration.getTransportStrategy().getGrpcConfiguration().getDeadline();
    this.numUnaryGrpcChannels =
        configuration.getTransportStrategy().getGrpcConfiguration().getNumUnaryGrpcChannels();
    this.numStreamGrpcChannels =
        configuration.getTransportStrategy().getGrpcConfiguration().getNumStreamGrpcChannels();

    this.unaryChannels =
        IntStream.range(0, this.numUnaryGrpcChannels)
            .mapToObj(i -> setupConnection(credentialProvider, configuration))
            .collect(Collectors.toList());
    this.unaryStubs = unaryChannels.stream().map(PubsubGrpc::newStub).collect(Collectors.toList());

    this.streamChannels =
        IntStream.range(0, this.numStreamGrpcChannels)
            .mapToObj(i -> setupConnection(credentialProvider, configuration))
            .collect(Collectors.toList());
    this.streamStubs =
        streamChannels.stream().map(PubsubGrpc::newStub).collect(Collectors.toList());
  }

  private static ManagedChannel setupConnection(
      CredentialProvider credentialProvider, TopicConfiguration configuration) {
    final NettyChannelBuilder channelBuilder =
        NettyChannelBuilder.forAddress(
            credentialProvider.getCacheEndpoint(), credentialProvider.getPort());

    // set additional channel options (message size, keepalive, auth, etc)
    GrpcChannelOptions.applyGrpcConfigurationToChannelBuilder(
        configuration.getTransportStrategy().getGrpcConfiguration(),
        channelBuilder,
        credentialProvider.isEndpointSecure());

    final List<ClientInterceptor> clientInterceptors = new ArrayList<>();

    final List<Middleware> middlewares = configuration.getMiddlewares();
    final MiddlewareRequestHandlerContext context =
        () -> Collections.singletonMap(CONNECTION_ID_KEY.toString(), UUID.randomUUID().toString());
    clientInterceptors.add(new GrpcMiddlewareInterceptor(middlewares, context));

    clientInterceptors.add(new UserHeaderInterceptor(credentialProvider.getAuthToken(), "topic"));
    channelBuilder.intercept(clientInterceptors);
    return channelBuilder.build();
  }

  /** Round-robin publish stub. */
  PubsubGrpc.PubsubStub getNextUnaryStub() {
    return unaryStubs
        .get(unaryIndex.getAndIncrement() % this.numUnaryGrpcChannels)
        .withDeadlineAfter(deadline.toMillis(), TimeUnit.MILLISECONDS);
  }

  /** Round-robin subscribe stub. */
  PubsubGrpc.PubsubStub getNextStreamStub() {
    return streamStubs.get(streamIndex.getAndIncrement() % this.numStreamGrpcChannels);
  }

  TopicConfiguration getConfiguration() {
    return configuration;
  }

  @Override
  public void close() {
    unaryChannels.forEach(ManagedChannel::shutdown);
    streamChannels.forEach(ManagedChannel::shutdown);
  }
}
