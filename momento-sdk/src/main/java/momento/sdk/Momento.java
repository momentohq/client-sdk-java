package momento.sdk;

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
import momento.sdk.messages.CreateCacheResponse;

public final class Momento implements Closeable {

  private static final String CONTROL_ENDPOINT_PREFIX = "control.";
  private static final String CACHE_ENDPOINT_PREFIX = "cache.";

  private final String authToken;
  private final String hostedZone;
  private final ScsControlBlockingStub blockingStub;
  private final ManagedChannel channel;

  private Momento(String authToken, String hostedZone) {
    this.authToken = authToken;
    this.hostedZone = hostedZone;
    NettyChannelBuilder channelBuilder =
        NettyChannelBuilder.forAddress(CONTROL_ENDPOINT_PREFIX + hostedZone, 443);
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
      return new CreateCacheResponse(makeCacheClient(authToken, cacheName, hostedZone));
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

  public Cache getCache(String cacheName) {
    checkCacheNameValid(cacheName);
    return makeCacheClient(authToken, cacheName, hostedZone);
  }

  private CreateCacheRequest buildCreateCacheRequest(String cacheName) {
    return CreateCacheRequest.newBuilder().setCacheName(cacheName).build();
  }

  private static void checkCacheNameValid(String cacheName) {
    if (cacheName == null) {
      throw new ClientSdkException("Cache Name is required.");
    }
  }

  private static Cache makeCacheClient(String authToken, String cacheName, String endpoint) {
    return new Cache(authToken, cacheName, CACHE_ENDPOINT_PREFIX + endpoint);
  }

  public void close() {
    this.channel.shutdown();
  }

  public static MomentoBuilder builder() {
    return new MomentoBuilder();
  }

  // TODO: ParseJWT to determine the authToken
  private static String extractEndpoint(String authToken) {
    return null;
  }

  public static class MomentoBuilder {
    private String authToken;
    private String endpointOverride;

    public MomentoBuilder authToken(String authToken) {
      this.authToken = authToken;
      return this;
    }

    /**
     * Endpoint that will be used to perform Momento Cache Operations.
     *
     * @param endpointOverride
     * @return
     */
    // TODO: Write a better public facing doc, this is basically a hosted zone for the cell against
    // which the requests
    // will be made.
    public MomentoBuilder endpointOverride(String endpointOverride) {
      this.endpointOverride = endpointOverride;
      return this;
    }

    public Momento build() {
      if (authToken == null || authToken.isEmpty()) {
        throw new ClientSdkException("Auth Token is required");
      }
      // Endpoint must be either available in the authToken or must be provided via
      // endPointOverride.
      String endpoint =
          endpointOverride != null && !endpointOverride.isEmpty()
              ? endpointOverride
              : extractEndpoint(authToken);

      if (endpoint == null) {
        throw new ClientSdkException("Endpoint for cache service is a required parameter.");
      }
      return new Momento(authToken, endpoint);
    }
  }
}
