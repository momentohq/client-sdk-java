package momento.sdk;

import static momento.sdk.ValidationUtils.checkCacheNameValid;

import grpc.control_client.CreateCacheRequest;
import grpc.control_client.DeleteCacheRequest;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import momento.sdk.exceptions.CacheServiceExceptionMapper;
import momento.sdk.messages.CacheInfo;
import momento.sdk.messages.CreateCacheResponse;
import momento.sdk.messages.DeleteCacheResponse;
import momento.sdk.messages.ListCachesResponse;
import org.apache.commons.lang3.StringUtils;

/** Client for interacting with Scs Control Plane. */
final class ScsControlClient implements Closeable {

  private final ScsControlGrpcStubsManager controlGrpcStubsManager;

  ScsControlClient(String authToken, String endpoint) {
    this.controlGrpcStubsManager = new ScsControlGrpcStubsManager(authToken, endpoint);
  }

  CreateCacheResponse createCache(String cacheName) {
    checkCacheNameValid(cacheName);
    try {
      controlGrpcStubsManager.getBlockingStub().createCache(buildCreateCacheRequest(cacheName));
      return new CreateCacheResponse();
    } catch (Exception e) {
      throw CacheServiceExceptionMapper.convert(e);
    }
  }

  DeleteCacheResponse deleteCache(String cacheName) {
    checkCacheNameValid(cacheName);
    try {
      controlGrpcStubsManager.getBlockingStub().deleteCache(buildDeleteCacheRequest(cacheName));
      return new DeleteCacheResponse();
    } catch (Exception e) {
      throw CacheServiceExceptionMapper.convert(e);
    }
  }

  ListCachesResponse listCaches(Optional<String> nextToken) {
    try {
      return convert(controlGrpcStubsManager.getBlockingStub().listCaches(convert(nextToken)));
    } catch (Exception e) {
      throw CacheServiceExceptionMapper.convert(e);
    }
  }

  private static CreateCacheRequest buildCreateCacheRequest(String cacheName) {
    return CreateCacheRequest.newBuilder().setCacheName(cacheName).build();
  }

  private static DeleteCacheRequest buildDeleteCacheRequest(String cacheName) {
    return DeleteCacheRequest.newBuilder().setCacheName(cacheName).build();
  }

  private static grpc.control_client.ListCachesRequest convert(Optional<String> nextToken) {
    String grpcNextToken = nextToken == null || !nextToken.isPresent() ? "" : nextToken.get();
    return grpc.control_client.ListCachesRequest.newBuilder().setNextToken(grpcNextToken).build();
  }

  private static ListCachesResponse convert(grpc.control_client.ListCachesResponse response) {
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

  private static CacheInfo convert(grpc.control_client.Cache cache) {
    return new CacheInfo(cache.getCacheName());
  }

  @Override
  public void close() {
    controlGrpcStubsManager.close();
  }
}
