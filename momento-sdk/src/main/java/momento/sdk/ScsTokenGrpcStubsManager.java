package momento.sdk;

import io.grpc.ClientInterceptor;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import momento.sdk.auth.CredentialProvider;
import momento.sdk.config.transport.GrpcConfiguration;
import momento.sdk.internal.GrpcChannelOptions;
import momento.token.TokenGrpc;

/**
 * Manager responsible for GRPC channels and stubs for the Disposable Token.
 *
 * <p>The business layer, will get request stubs from this layer. This keeps the two layers
 * independent and any future pooling of channels can happen exclusively in the manager without
 * impacting the API business logic.
 */
final class ScsTokenGrpcStubsManager implements AutoCloseable {

  private static final Duration DEADLINE = Duration.ofMinutes(1);

  private final ManagedChannel channel;

  private final TokenGrpc.TokenFutureStub futureStub;

  ScsTokenGrpcStubsManager(@Nonnull CredentialProvider credentialProvider) {
    this.channel = setupConnection(credentialProvider);
    this.futureStub = TokenGrpc.newFutureStub(channel);
  }

  private static ManagedChannel setupConnection(CredentialProvider credentialProvider) {
    final NettyChannelBuilder channelBuilder =
        NettyChannelBuilder.forAddress(credentialProvider.getTokenEndpoint(), 443);

    // Note: This is hard-coded for now but we may want to expose it via configuration object
    // in the future, as we do with some of the other clients.
    final GrpcConfiguration grpcConfig = new GrpcConfiguration(Duration.ofMillis(15000));

    // set additional channel options (message size, keepalive, auth, etc)
    GrpcChannelOptions.applyGrpcConfigurationToChannelBuilder(grpcConfig, channelBuilder);

    final List<ClientInterceptor> clientInterceptors = new ArrayList<>();
    clientInterceptors.add(new UserHeaderInterceptor(credentialProvider.getAuthToken(), "auth"));
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
  TokenGrpc.TokenFutureStub getStub() {
    return futureStub.withDeadlineAfter(DEADLINE.toMillis(), TimeUnit.MILLISECONDS);
  }

  @Override
  public void close() {
    channel.shutdown();
  }
}
