package momento.sdk;

import static java.util.Optional.empty;

import grpc.control_client.CreateCacheRequest;
import grpc.control_client.ScsControlGrpc;
import grpc.control_client.ScsControlGrpc.ScsControlBlockingStub;
import io.grpc.ClientInterceptor;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.netty.NettyChannelBuilder;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import momento.sdk.exceptions.CacheAlreadyExistsException;
import momento.sdk.exceptions.CacheServiceExceptionMapper;
import momento.sdk.exceptions.ClientSdkException;

public final class Momento implements Closeable {

  private final String authToken;
  private final ScsControlBlockingStub blockingStub;
  private final ManagedChannel channel;

  public static Momento init(String authToken) {
    if (authToken == null) {
      throw new ClientSdkException("Auth Token is required to make a request.");
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

  /**
   * Creates a cache with provided name
   *
   * @param cacheName
   * @return {@link Cache} that allows consumers to perform cache operations
   * @throws {@link momento.sdk.exceptions.PermissionDeniedException} - if provided authToken is
   *     invalid <br>
   *     {@link CacheAlreadyExistsException} - if Cache with the same name exists <br>
   *     {@link momento.sdk.exceptions.InternalServerException} - for any unexpected errors that
   *     occur on the service side.<br>
   *     {@link ClientSdkException} - for any client side errors
   */
  public Cache createCache(String cacheName) {
    checkCacheNameValid(cacheName);
    try {
      this.blockingStub.createCache(buildCreateCacheRequest(cacheName));
      return buildCache(cacheName);
    } catch (io.grpc.StatusRuntimeException e) {
      if (e.getStatus() == Status.ALREADY_EXISTS) {
        throw new CacheAlreadyExistsException(
            String.format("Cache with name %s already exists", cacheName));
      }
      throw CacheServiceExceptionMapper.convert(e);
    } catch (Exception e) {
      throw CacheServiceExceptionMapper.convert(e);
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
    if (cacheName == null) {
      throw new ClientSdkException("Cache Name is required.");
    }
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
