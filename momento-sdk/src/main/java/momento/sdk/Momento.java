package momento.sdk;

import grpc.control_client.CreateCacheRequest;
import grpc.control_client.DeleteCacheRequest;
import grpc.control_client.ScsControlGrpc;
import grpc.control_client.ScsControlGrpc.ScsControlBlockingStub;
import io.grpc.ClientInterceptor;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.netty.NettyChannelBuilder;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import momento.sdk.exceptions.CacheAlreadyExistsException;
import momento.sdk.exceptions.CacheServiceExceptionMapper;
import momento.sdk.exceptions.ClientSdkException;
import momento.sdk.messages.CreateCacheResponse;
import momento.sdk.messages.DeleteCacheResponse;

public final class Momento implements Closeable {

  private final String authToken;
  private final ScsControlBlockingStub blockingStub;
  private final ManagedChannel channel;
  private final MomentoEndpointsResolver.MomentoEndpoints momentoEndpoints;

  private Momento(String authToken, Optional<String> hostedZoneOverride) {

    this.authToken = authToken;
    this.momentoEndpoints = MomentoEndpointsResolver.resolve(authToken, hostedZoneOverride);
    this.channel = setupConnection(momentoEndpoints, authToken);
    this.blockingStub = ScsControlGrpc.newBlockingStub(channel);
  }

  private static ManagedChannel setupConnection(
      MomentoEndpointsResolver.MomentoEndpoints momentoEndpoints, String authToken) {
    NettyChannelBuilder channelBuilder =
        NettyChannelBuilder.forAddress(momentoEndpoints.controlEndpoint(), 443);
    channelBuilder.useTransportSecurity();
    channelBuilder.disableRetry();
    List<ClientInterceptor> clientInterceptors = new ArrayList<>();
    clientInterceptors.add(new AuthInterceptor(authToken));
    channelBuilder.intercept(clientInterceptors);
    return channelBuilder.build();
  }

  /**
   * Creates a cache with provided name
   *
   * @param cacheName
   * @return {@link CreateCacheResponse} that allows consumers to perform cache operations
   * @throws {@link momento.sdk.exceptions.PermissionDeniedException} - if provided authToken is
   *     invalid <br>
   *     {@link CacheAlreadyExistsException} - if Cache with the same name exists <br>
   *     {@link momento.sdk.exceptions.InternalServerException} - for any unexpected errors that
   *     occur on the service side.<br>
   *     {@link ClientSdkException} - for any client side errors
   */
  public CreateCacheResponse createCache(String cacheName) {
    checkCacheNameValid(cacheName);
    try {
      this.blockingStub.createCache(buildCreateCacheRequest(cacheName));
      return new CreateCacheResponse();
    } catch (io.grpc.StatusRuntimeException e) {
      if (e.getStatus().getCode() == Status.Code.ALREADY_EXISTS) {
        throw new CacheAlreadyExistsException(
            String.format("Cache with name %s already exists", cacheName));
      }
      throw CacheServiceExceptionMapper.convert(e);
    } catch (Exception e) {
      throw CacheServiceExceptionMapper.convert(e);
    }
  }

  /**
   * Deletes a cache with provided name
   *
   * @param cacheName
   * @return {@link DeleteCacheResponse} that allows consumers to perform cache operations
   * @throws {@link momento.sdk.exceptions.PermissionDeniedException} - if provided authToken is
   *     invalid <br>
   *     {@link momento.sdk.exceptions.InternalServerException} - for any unexpected errors that
   *     occur on the service side.<br>
   *     {@link ClientSdkException} - for any client side errors
   */
  public DeleteCacheResponse deleteCache(String cacheName) {
    checkCacheNameValid(cacheName);
    try {
      this.blockingStub.deleteCache(buildDeleteCacheRequest(cacheName));
      return new DeleteCacheResponse();
    } catch (Exception e) {
      throw CacheServiceExceptionMapper.convert(e);
    }
  }

  public Cache getCache(String cacheName) {
    checkCacheNameValid(cacheName);
    return makeCacheClient(authToken, cacheName, momentoEndpoints.cacheEndpoint());
  }

  private CreateCacheRequest buildCreateCacheRequest(String cacheName) {
    return CreateCacheRequest.newBuilder().setCacheName(cacheName).build();
  }

  private DeleteCacheRequest buildDeleteCacheRequest(String cacheName) {
    return DeleteCacheRequest.newBuilder().setCacheName(cacheName).build();
  }

  private static void checkCacheNameValid(String cacheName) {
    if (cacheName == null) {
      throw new ClientSdkException("Cache Name is required.");
    }
  }

  private static Cache makeCacheClient(String authToken, String cacheName, String endpoint) {
    return new Cache(authToken, cacheName, endpoint);
  }

  public void close() {
    this.channel.shutdown();
  }

  public static MomentoBuilder builder(String authToken) {
    return new MomentoBuilder(authToken);
  }

  public static class MomentoBuilder {
    private String authToken;
    private Optional<String> endpointOverride = Optional.empty();

    public MomentoBuilder(String authToken) {
      this.authToken = authToken;
    }

    /**
     * Endpoint that will be used to perform Momento Cache Operations.
     *
     * <p>This should be set only if Momento Team has provided you one.
     */
    public MomentoBuilder endpointOverride(String endpointOverride) {
      this.endpointOverride = Optional.ofNullable(endpointOverride);
      return this;
    }

    public Momento build() {
      if (authToken == null || authToken.isEmpty()) {
        throw new ClientSdkException("Auth Token is required");
      }
      return new Momento(authToken, endpointOverride);
    }
  }
}
