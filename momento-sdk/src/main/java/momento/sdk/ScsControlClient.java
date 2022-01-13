package momento.sdk;

import grpc.control_client.CreateCacheRequest;
import grpc.control_client.DeleteCacheRequest;
import io.grpc.Status;
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

  DeleteCacheResponse deleteCache(String cacheName) {
    checkCacheNameValid(cacheName);
    try {
      controlGrpcStubsManager.getBlockingStub().deleteCache(buildDeleteCacheRequest(cacheName));
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

  ListCachesResponse listCaches(ListCachesRequest request) {
    try {
      return convert(controlGrpcStubsManager.getBlockingStub().listCaches(convert(request)));
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

  private static grpc.control_client.ListCachesRequest convert(ListCachesRequest request) {
    return grpc.control_client.ListCachesRequest.newBuilder()
        .setNextToken(request.nextPageToken().orElse(""))
        .build();
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

  private static void checkCacheNameValid(String cacheName) {
    if (cacheName == null) {
      throw new ClientSdkException("Cache Name is required.");
    }
  }

  @Override
  public void close() {
    controlGrpcStubsManager.close();
  }
}
