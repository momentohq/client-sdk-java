package momento.sdk;

import grpc.leaderboard.LeaderboardGrpc;
import io.grpc.ClientInterceptor;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nonnull;
import momento.sdk.auth.CredentialProvider;
import momento.sdk.config.LeaderboardConfiguration;
import momento.sdk.interceptors.UserHeaderInterceptor;
import momento.sdk.internal.GrpcChannelOptions;

/** Manager responsible for GRPC channels and stubs for leaderboards. */
final class LeaderboardGrpcStubsManager implements AutoCloseable {

  private final List<ManagedChannel> channels;
  private final List<LeaderboardGrpc.LeaderboardFutureStub> futureStubs;
  private final AtomicInteger nextStubIndex = new AtomicInteger(0);

  private final int numGrpcChannels;
  private final Duration deadline;

  LeaderboardGrpcStubsManager(
      @Nonnull CredentialProvider credentialProvider,
      @Nonnull LeaderboardConfiguration configuration) {
    this.deadline = configuration.getTransportStrategy().getGrpcConfiguration().getDeadline();
    this.numGrpcChannels =
        configuration.getTransportStrategy().getGrpcConfiguration().getMinNumGrpcChannels();

    this.channels =
        IntStream.range(0, this.numGrpcChannels)
            .mapToObj(i -> setupChannel(credentialProvider, configuration))
            .collect(Collectors.toList());
    this.futureStubs =
        channels.stream().map(LeaderboardGrpc::newFutureStub).collect(Collectors.toList());
  }

  private static ManagedChannel setupChannel(
      CredentialProvider credentialProvider, LeaderboardConfiguration configuration) {
    final NettyChannelBuilder channelBuilder =
        NettyChannelBuilder.forAddress(credentialProvider.getCacheEndpoint(), 443);

    // set additional channel options (message size, keepalive, auth, etc)
    GrpcChannelOptions.applyGrpcConfigurationToChannelBuilder(
        configuration.getTransportStrategy().getGrpcConfiguration(), channelBuilder);

    final List<ClientInterceptor> clientInterceptors = new ArrayList<>();
    clientInterceptors.add(
        new UserHeaderInterceptor(credentialProvider.getAuthToken(), "leaderboard"));
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
  LeaderboardGrpc.LeaderboardFutureStub getStub() {
    int nextStubIndex = this.nextStubIndex.getAndIncrement();
    return futureStubs
        .get(nextStubIndex % this.numGrpcChannels)
        .withDeadlineAfter(deadline.toMillis(), TimeUnit.MILLISECONDS);
  }

  @Override
  public void close() {
    for (ManagedChannel channel : channels) {
      channel.shutdown();
    }
  }
}
