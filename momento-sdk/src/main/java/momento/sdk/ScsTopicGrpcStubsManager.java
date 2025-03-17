package momento.sdk;

import grpc.cache_client.pubsub.PubsubGrpc;
import io.grpc.ClientInterceptor;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
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

  private final ManagedChannel channel;
  private final PubsubGrpc.PubsubStub stub;
  public static final UUID CONNECTION_ID_KEY = UUID.randomUUID();

  private final TopicConfiguration configuration;

  ScsTopicGrpcStubsManager(
      @Nonnull CredentialProvider credentialProvider, @Nonnull TopicConfiguration configuration) {
    this.channel = setupConnection(credentialProvider, configuration);
    this.stub = PubsubGrpc.newStub(channel);
    this.configuration = configuration;
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

  /** Returns a pubsub stub. */
  PubsubGrpc.PubsubStub getStub() {
    return stub;
  }

  TopicConfiguration getConfiguration() {
    return configuration;
  }

  @Override
  public void close() {
    channel.shutdown();
  }
}
