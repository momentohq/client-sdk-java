package momento.sdk;

import grpc.cache_client.pubsub.PubsubGrpc;
import io.grpc.ClientInterceptor;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import momento.sdk.auth.CredentialProvider;
import momento.sdk.config.TopicConfiguration;
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
        NettyChannelBuilder.forAddress(credentialProvider.getCacheEndpoint(), 443);

    // set additional channel options (message size, keepalive, auth, etc)
    GrpcChannelOptions.applyGrpcConfigurationToChannelBuilder(
        configuration.getTransportStrategy().getGrpcConfiguration(), channelBuilder);

    final List<ClientInterceptor> clientInterceptors = new ArrayList<>();
    clientInterceptors.add(new UserHeaderInterceptor(credentialProvider.getAuthToken()));
    channelBuilder.intercept(clientInterceptors);
    return channelBuilder.build();
  }

  /**
   * Returns a stub with appropriate deadlines.
   *
   * <p>Each stub is deliberately decorated with Deadline. Deadlines work differently than timeouts.
   * When a deadline is set on a stub, it simply means that once the stub is created it must be used
   * before the deadline expires. Hence, the stub returned from here should never be cached and the
   * safest behavior is for clients to request a new stub each time.
   *
   * <p><a href="https://github.com/grpc/grpc-java/issues/1495">more information</a>
   */
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
