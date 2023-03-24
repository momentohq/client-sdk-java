package momento.sdk;

import grpc.control_client.ScsControlGrpc;
import io.grpc.ClientInterceptor;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.io.Closeable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;

/**
 * Manager responsible for GRPC channels and stubs for the Control Plane.
 *
 * <p>The business layer, will get request stubs from this layer. This keeps the two layers
 * independent and any future pooling of channels can happen exclusively in the manager without
 * impacting the API business logic.
 */
final class ScsControlGrpcStubsManager implements Closeable {

  private static final Duration DEADLINE = Duration.ofMinutes(1);

  private final ManagedChannel channel;
  private final ScsControlGrpc.ScsControlBlockingStub controlBlockingStub;

  ScsControlGrpcStubsManager(@Nonnull String authToken, @Nonnull String endpoint) {
    this.channel = setupConnection(authToken, endpoint);
    this.controlBlockingStub = ScsControlGrpc.newBlockingStub(channel);
  }

  private static ManagedChannel setupConnection(String authToken, String endpoint) {
    NettyChannelBuilder channelBuilder = NettyChannelBuilder.forAddress(endpoint, 443);
    channelBuilder.useTransportSecurity();
    channelBuilder.disableRetry();
    List<ClientInterceptor> clientInterceptors = new ArrayList<>();
    clientInterceptors.add(new UserHeaderInterceptor(authToken));
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
  ScsControlGrpc.ScsControlBlockingStub getBlockingStub() {
    return controlBlockingStub.withDeadlineAfter(DEADLINE.getSeconds(), TimeUnit.SECONDS);
  }

  /** Return the length in seconds of the deadline that a newly created stub will have. */
  public long getDeadlineSeconds() {
    return DEADLINE.getSeconds();
  }

  @Override
  public void close() {
    channel.shutdown();
  }
}
