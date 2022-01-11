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
import momento.sdk.messages.CacheInfo;
import momento.sdk.messages.CreateCacheResponse;
import momento.sdk.messages.DeleteCacheResponse;
import momento.sdk.messages.ListCachesRequest;
import momento.sdk.messages.ListCachesResponse;
import org.apache.commons.lang3.StringUtils;

/** Client to interact with Momento services. */
public final class Momento implements Closeable {

  private final String authToken;
  // TODO: Add future stub
  private final ScsControlBlockingStub blockingStub;
  private final ManagedChannel channel;
  private final MomentoEndpointsResolver.MomentoEndpoints momentoEndpoints;
  private final ScsGrpcClient scsGrpcClient;

  private Momento(String authToken, Optional<String> hostedZoneOverride) {
    this.authToken = authToken;
    this.momentoEndpoints = MomentoEndpointsResolver.resolve(authToken, hostedZoneOverride);
    this.channel = setupConnection(momentoEndpoints, authToken);
    this.blockingStub = ScsControlGrpc.newBlockingStub(channel);
    this.scsGrpcClient = new ScsGrpcClient(authToken, momentoEndpoints.cacheEndpoint());
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

  /** Lists all caches for the provided auth token. */
  public ListCachesResponse listCaches(ListCachesRequest request) {
    try {
      return convert(this.blockingStub.listCaches(convert(request)));
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
   * @return {@link CacheClientBuilder} to further build the {@link Cache} client.
   */
  public CacheClientBuilder cacheBuilder(String cacheName, int defaultItemTtlSeconds) {
    return new CacheClientBuilder(this, cacheName, defaultItemTtlSeconds);
  }

  private CreateCacheRequest buildCreateCacheRequest(String cacheName) {
    return CreateCacheRequest.newBuilder().setCacheName(cacheName).build();
  }

  private DeleteCacheRequest buildDeleteCacheRequest(String cacheName) {
    return DeleteCacheRequest.newBuilder().setCacheName(cacheName).build();
  }

  private grpc.control_client.ListCachesRequest convert(ListCachesRequest request) {
    return grpc.control_client.ListCachesRequest.newBuilder()
        .setNextToken(request.nextPageToken().orElse(""))
        .build();
  }

  private ListCachesResponse convert(grpc.control_client.ListCachesResponse response) {
    List<CacheInfo> caches = new ArrayList<>();
    for (grpc.control_client.Cache cache : response.getCacheList()) {
      caches.add(convert(cache));
    }
    Optional<String> nextPageToken =
        StringUtils.isEmpty(response.getNextToken())
            ? Optional.empty()
            : Optional.of(response.getNextToken());
    return new ListCachesResponse(caches, nextPageToken);
  }

  private CacheInfo convert(grpc.control_client.Cache cache) {
    return new CacheInfo(cache.getCacheName());
  }

  static void checkCacheNameValid(String cacheName) {
    if (cacheName == null) {
      throw new ClientSdkException("Cache Name is required.");
    }
  }

  ScsGrpcClient getScsClient() {
    return scsGrpcClient;
  }

  /** Shuts down the client. */
  public void close() {
    channel.shutdown();
    scsGrpcClient.close();
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

  /** Builder for {@link Momento} client */
  public static class MomentoBuilder {
    private String authToken;
    private Optional<String> endpointOverride = Optional.empty();

    private MomentoBuilder(String authToken) {
      this.authToken = authToken;
    }

    /**
     * Override the endpoints used to perform operations.
     *
     * <p>This parameter should only be set when Momento services team advises to. Any invalid
     * values here will result in application failures.
     *
     * @param endpointOverride Endpoint for momento services.
     */
    public MomentoBuilder endpointOverride(String endpointOverride) {
      this.endpointOverride = Optional.ofNullable(endpointOverride);
      return this;
    }

    /**
     * Creates a {@link momento.sdk.Momento} client.
     *
     * @throws ClientSdkException for malformed auth tokens or other invalid data provided to
     *     initialize the client.
     */
    public Momento build() {
      if (StringUtils.isEmpty(authToken)) {
        throw new ClientSdkException("Auth Token is required");
      }
      return new Momento(authToken, endpointOverride);
    }
  }
}
