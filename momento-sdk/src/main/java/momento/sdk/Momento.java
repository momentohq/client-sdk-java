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
import momento.sdk.exceptions.CacheNotFoundException;
import momento.sdk.exceptions.CacheServiceExceptionMapper;
import momento.sdk.exceptions.ClientSdkException;
import momento.sdk.messages.CreateCacheResponse;
import momento.sdk.messages.DeleteCacheResponse;

/** Client to interact with Momento services. */
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
   * @param cacheName Name of the cache to be created.
   * @return The result of the create cache operation
   * @throws momento.sdk.exceptions.PermissionDeniedException
   * @throws momento.sdk.exceptions.InvalidArgumentException
   * @throws CacheAlreadyExistsException
   * @throws momento.sdk.exceptions.InternalServerException
   * @throws ClientSdkException when cacheName is null
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
   * Deletes a cache
   *
   * @param cacheName The name of the cache to be deleted.
   * @return The result of the cache deletion operation.
   * @throws momento.sdk.exceptions.PermissionDeniedException
   * @throws CacheNotFoundException
   * @throws momento.sdk.exceptions.InternalServerException
   * @throws ClientSdkException if the {@code cacheName} is null.
   */
  public DeleteCacheResponse deleteCache(String cacheName) {
    checkCacheNameValid(cacheName);
    try {
      this.blockingStub.deleteCache(buildDeleteCacheRequest(cacheName));
      return new DeleteCacheResponse();
    } catch (io.grpc.StatusRuntimeException e) {
      if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
        throw new CacheNotFoundException(
            String.format("Cache with name %s doesn't exist", cacheName));
      }
      throw CacheServiceExceptionMapper.convert(e);
    } catch (Exception e) {
      throw CacheServiceExceptionMapper.convert(e);
    }
  }

  /**
   * Creates a builder to make a Cache client.
   *
   * @param cacheName - Name of the cache for the which the client will be built.
   * @param defaultItemTtlSeconds - The default Time to live in seconds for the items that will be
   *     stored in Cache. Default TTL can be overridden at individual items level at the time of
   *     storing them in the cache.
   * @see CacheClientBuilder
   */
  public CacheClientBuilder cacheBuilder(String cacheName, int defaultItemTtlSeconds) {
    return new CacheClientBuilder(
        this, authToken, cacheName, defaultItemTtlSeconds, momentoEndpoints.cacheEndpoint());
  }

  private CreateCacheRequest buildCreateCacheRequest(String cacheName) {
    return CreateCacheRequest.newBuilder().setCacheName(cacheName).build();
  }

  private DeleteCacheRequest buildDeleteCacheRequest(String cacheName) {
    return DeleteCacheRequest.newBuilder().setCacheName(cacheName).build();
  }

  static void checkCacheNameValid(String cacheName) {
    if (cacheName == null) {
      throw new ClientSdkException("Cache Name is required.");
    }
  }

  /** Shuts down the client. */
  public void close() {
    this.channel.shutdown();
  }

  /**
   * Builder to create a {@link Momento} client.
   *
   * @param authToken The authentication token required to authenticate with Momento Services.
   * @return A builder to build the Momento Client
   */
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
