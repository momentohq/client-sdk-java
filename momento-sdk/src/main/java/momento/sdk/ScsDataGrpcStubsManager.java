package momento.sdk;

import grpc.cache_client.ScsGrpc;
import io.grpc.ClientInterceptor;
import io.grpc.Deadline;
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

  ScsDataGrpcStubsManager(
      String authToken,
      String endpoint,
      Optional<OpenTelemetry> openTelemetry,
      Optional<Duration> requestTimeout) {
    Duration deadline = requestTimeout.orElse(DEFAULT_DEADLINE);
    this.channel = setupChannel(authToken, endpoint, openTelemetry);
    this.futureStub =
        ScsGrpc.newFutureStub(channel)
            .withDeadline(Deadline.after(deadline.getSeconds(), TimeUnit.SECONDS));
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

  ScsGrpc.ScsFutureStub getStub() {
    return futureStub;
  }

  @Override
  public void close() {
    channel.shutdown();
  }
}
