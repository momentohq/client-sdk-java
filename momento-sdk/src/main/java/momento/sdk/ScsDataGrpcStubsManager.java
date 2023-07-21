package momento.sdk;

import grpc.cache_client.ScsGrpc;
import io.grpc.ClientInterceptor;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.io.Closeable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import momento.sdk.auth.CredentialProvider;
import momento.sdk.config.Configuration;

/**
 * Manager responsible for GRPC channels and stubs for the Data Plane.
 *
 * <p>The business layer, will get request stubs from this layer. This keeps the two layers
 * independent and any future pooling of channels can happen exclusively in the manager without
 * impacting the API business logic.
 */
final class ScsDataGrpcStubsManager implements Closeable {

  private final ManagedChannel channel;
  private final ScsGrpc.ScsFutureStub futureStub;
  private final Duration deadline;

  ScsDataGrpcStubsManager(
      @Nonnull CredentialProvider credentialProvider, @Nonnull Configuration configuration) {
    this.deadline = configuration.getTransportStrategy().getGrpcConfiguration().getDeadline();

    this.channel = setupChannel(credentialProvider, configuration);
    this.futureStub = ScsGrpc.newFutureStub(channel);
  }

  private static ManagedChannel setupChannel(
      CredentialProvider credentialProvider, Configuration configuration) {
    final NettyChannelBuilder channelBuilder =
        NettyChannelBuilder.forAddress(credentialProvider.getCacheEndpoint(), 443);
    channelBuilder.useTransportSecurity();
    channelBuilder.disableRetry();
    final List<ClientInterceptor> clientInterceptors = new ArrayList<>();
    clientInterceptors.add(new UserHeaderInterceptor(credentialProvider.getAuthToken()));
    clientInterceptors.add(
        new RetryClientInterceptor(
            configuration.getRetryStrategy(), configuration.getRetryEligibilityStrategy()));
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
  ScsGrpc.ScsFutureStub getStub() {
    return futureStub.withDeadlineAfter(deadline.getSeconds(), TimeUnit.SECONDS);
  }

  @Override
  public void close() {
    channel.shutdown();
  }
}
