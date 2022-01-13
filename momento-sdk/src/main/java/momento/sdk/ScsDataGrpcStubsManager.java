package momento.sdk;

import grpc.cache_client.ScsGrpc;
import io.grpc.ClientInterceptor;
import io.grpc.ManagedChannel;
import io.grpc.netty.NettyChannelBuilder;
import io.opentelemetry.api.OpenTelemetry;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

final class ScsDataGrpcStubsManager implements Closeable {

  private final ManagedChannel channel;
  private final ScsGrpc.ScsFutureStub futureStub;

  ScsDataGrpcStubsManager(
      String authToken, String endpoint, Optional<OpenTelemetry> openTelemetry) {
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

  ScsGrpc.ScsFutureStub getStub() {
    return futureStub;
  }

  @Override
  public void close() {
    channel.shutdown();
  }
}
