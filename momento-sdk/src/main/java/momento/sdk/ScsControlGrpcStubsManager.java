package momento.sdk;

import grpc.control_client.ScsControlGrpc;
import io.grpc.ClientInterceptor;
import io.grpc.ManagedChannel;
import io.grpc.netty.NettyChannelBuilder;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;

final class ScsControlGrpcStubsManager implements Closeable {

  private final ManagedChannel channel;
  private final ScsControlGrpc.ScsControlBlockingStub controlBlockingStub;

  ScsControlGrpcStubsManager(String authToken, String endpoint) {
    this.channel = setupConnection(authToken, endpoint);
    this.controlBlockingStub = ScsControlGrpc.newBlockingStub(channel);
  }

  private static ManagedChannel setupConnection(String authToken, String endpoint) {
    NettyChannelBuilder channelBuilder = NettyChannelBuilder.forAddress(endpoint, 443);
    channelBuilder.useTransportSecurity();
    channelBuilder.disableRetry();
    List<ClientInterceptor> clientInterceptors = new ArrayList<>();
    clientInterceptors.add(new AuthInterceptor(authToken));
    channelBuilder.intercept(clientInterceptors);
    return channelBuilder.build();
  }

  ScsControlGrpc.ScsControlBlockingStub getBlockingStub() {
    return controlBlockingStub;
  }

  @Override
  public void close() {
    channel.shutdown();
  }
}
