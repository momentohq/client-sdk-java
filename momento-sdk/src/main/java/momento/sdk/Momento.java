package momento.sdk;

import static java.util.Optional.*;

import grpc.control_client.CreateCacheRequest;
import grpc.control_client.CreateCacheResponse;
import grpc.control_client.ScsControlGrpc;
import grpc.control_client.ScsControlGrpc.ScsControlBlockingStub;
import io.grpc.ClientInterceptor;
import io.grpc.ManagedChannel;
import io.grpc.netty.NettyChannelBuilder;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import momento.sdk.exceptions.ClientSdkException;
import momento.sdk.exceptions.SdkException;

public final class Momento implements Closeable {

  private final String authToken;
  private final ScsControlBlockingStub blockingStub;
  private final ManagedChannel channel;

  public static Momento init(String authToken) {
    if (authToken == null) {
      throw new ClientSdkException(new IllegalArgumentException("cannot pass a null authToken"));
    }
    return new Momento(authToken);
  }

  private Momento(String authToken) {
    this.authToken = authToken;
    // FIXME get endpoint from JWT claim
    NettyChannelBuilder channelBuilder =
        NettyChannelBuilder.forAddress("control.cell-alpha-dev.preprod.a.momentohq.com", 443);
    channelBuilder.useTransportSecurity();
    channelBuilder.disableRetry();
    List<ClientInterceptor> clientInterceptors = new ArrayList<>();
    clientInterceptors.add(new AuthInterceptor(authToken));
    channelBuilder.intercept(clientInterceptors);
    ManagedChannel channel = channelBuilder.build();
    this.blockingStub = ScsControlGrpc.newBlockingStub(channel);
    this.channel = channel;
  }

  public Cache createCache(String cacheName) {
    checkCacheNameValid(cacheName);
    try {
      CreateCacheResponse ignored =
          this.blockingStub.createCache(buildCreateCacheRequest(cacheName));
      return buildCache(cacheName);
    } catch (io.grpc.StatusRuntimeException e) {
      // FIXME in future return more granular exceptions based of status code and or perform client
      // retries
      throw new SdkException(e);
    }
  }

  public Cache getCache(String cacheName) {
    checkCacheNameValid(cacheName);
    return buildCache(cacheName);
  }

  private CreateCacheRequest buildCreateCacheRequest(String cacheName) {
    return CreateCacheRequest.newBuilder().setCacheName(cacheName).build();
  }

  private static void checkCacheNameValid(String cacheName) {
    if (cacheName == null)
      throw new ClientSdkException(new IllegalArgumentException("null cacheName passed"));
  }

  private Cache buildCache(String cacheName) {
    return new Cache(
        this.authToken,
        cacheName,
        empty(),
        "cache.cell-alpha-dev.preprod.a.momentohq.com" // FIXME make this prop off jwt
        );
  }

  public void close() {
    this.channel.shutdown();
  }
}
