package momento.sdk;

import grpc.cache_client.ScsGrpc;
import io.grpc.ClientInterceptor;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.opentelemetry.api.OpenTelemetry;
import java.io.Closeable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Manager responsible for GRPC channels and stubs for the Data Plane.
 *
 * <p>The business layer, will get request stubs from this layer. This keeps the two layers
 * independent and any future pooling of channels can happen exclusively in the manager without
 * impacting the API business logic.
 */
final class ScsDataGrpcStubsManager implements Closeable {

  private static final Duration DEFAULT_DEADLINE = Duration.ofSeconds(5);

  private final ManagedChannel channel;
  private final ScsGrpc.ScsFutureStub futureStub;
  private final Duration deadline;

  ScsDataGrpcStubsManager(
      String authToken,
      String endpoint,
      Optional<OpenTelemetry> openTelemetry,
      Optional<Duration> requestTimeout) {
    this.deadline = requestTimeout.orElse(DEFAULT_DEADLINE);
    this.channel = setupChannel(authToken, endpoint, openTelemetry);
    this.futureStub = ScsGrpc.newFutureStub(channel);
  }

  private static ManagedChannel setupChannel(
      String authToken, String endpoint, Optional<OpenTelemetry> openTelemetry) {
    NettyChannelBuilder channelBuilder = NettyChannelBuilder.forAddress(endpoint, 443);
    channelBuilder.useTransportSecurity();
    channelBuilder.disableRetry();
    List<ClientInterceptor> clientInterceptors = new ArrayList<>();
    clientInterceptors.add(new AuthInterceptor(authToken));
    openTelemetry.ifPresent(
        theOpenTelemetry ->
            clientInterceptors.add(new OpenTelemetryClientInterceptor(theOpenTelemetry)));
    channelBuilder.intercept(clientInterceptors);
    ManagedChannel channel = channelBuilder.build();
    return channel;
  }

  /**
   * Returns a stub with appropriate deadlines.
   *
   * <p>Each stub is deliberately decorated with Deadline. Deadlines work differently than timeouts.
   * When a deadline is set on a stub, it simply means that once the stub is created it must be used
   * before the deadline expires. Hence, the stub returned from here should never be cached and the
   * safest behavior is for clients to request a new stub each time.
   *
   * <p>more information: https://github.com/grpc/grpc-java/issues/1495
   */
  ScsGrpc.ScsFutureStub getStub() {
    return futureStub.withDeadlineAfter(deadline.getSeconds(), TimeUnit.SECONDS);
  }

  @Override
  public void close() {
    channel.shutdown();
  }
}
