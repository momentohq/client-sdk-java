package momento.sdk;

import grpc.control_client.ScsControlGrpc;
import io.grpc.ClientInterceptor;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import momento.sdk.auth.CredentialProvider;
import momento.sdk.config.Configuration;
import momento.sdk.config.transport.GrpcConfiguration;
import momento.sdk.interceptors.UserHeaderInterceptor;
import momento.sdk.internal.GrpcChannelOptions;

/**
 * Manager responsible for GRPC channels and stubs for the Control Plane.
 *
 * <p>The business layer, will get request stubs from this layer. This keeps the two layers
 * independent and any future pooling of channels can happen exclusively in the manager without
 * impacting the API business logic.
 */
final class ScsControlGrpcStubsManager implements AutoCloseable {

  private static final Duration DEADLINE = Duration.ofMinutes(1);

  private final ManagedChannel channel;

  private final ScsControlGrpc.ScsControlFutureStub futureStub;

  ScsControlGrpcStubsManager(
      @Nonnull CredentialProvider credentialProvider, Configuration configuration) {
    this.channel = setupConnection(credentialProvider, configuration);
    this.futureStub = ScsControlGrpc.newFutureStub(channel);
  }

  private static ManagedChannel setupConnection(
      CredentialProvider credentialProvider, Configuration configuration) {
    int port = credentialProvider.getPort();
    final NettyChannelBuilder channelBuilder =
        NettyChannelBuilder.forAddress(credentialProvider.getControlEndpoint(), port);

    // Override grpc config to disable keepalive for control clients
    final GrpcConfiguration controlConfig =
        configuration.getTransportStrategy().getGrpcConfiguration().withKeepAliveDisabled();

    // set additional channel options (message size, keepalive, auth, etc)
    GrpcChannelOptions.applyGrpcConfigurationToChannelBuilder(
        controlConfig, channelBuilder, credentialProvider.isEndpointSecure());

    final List<ClientInterceptor> clientInterceptors = new ArrayList<>();
    clientInterceptors.add(new UserHeaderInterceptor(credentialProvider.getAuthToken(), "cache"));
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
  ScsControlGrpc.ScsControlFutureStub getStub() {
    return futureStub.withDeadlineAfter(DEADLINE.toMillis(), TimeUnit.MILLISECONDS);
  }

  @Override
  public void close() {
    channel.shutdown();
  }
}
