package momento.sdk;

import static momento.sdk.ValidationUtils.checkCacheNameValid;
import static momento.sdk.ValidationUtils.ensureValidTtlMinutes;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import grpc.control_client._Cache;
import grpc.control_client._CreateCacheRequest;
import grpc.control_client._CreateSigningKeyRequest;
import grpc.control_client._CreateSigningKeyResponse;
import grpc.control_client._DeleteCacheRequest;
import grpc.control_client._FlushCacheRequest;
import grpc.control_client._ListCachesRequest;
import grpc.control_client._ListCachesResponse;
import grpc.control_client._ListSigningKeysRequest;
import grpc.control_client._ListSigningKeysResponse;
import grpc.control_client._RevokeSigningKeyRequest;
import grpc.control_client._SigningKey;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import momento.sdk.exceptions.CacheServiceExceptionMapper;
import momento.sdk.messages.CacheInfo;
import momento.sdk.messages.CreateCacheResponse;
import momento.sdk.messages.CreateSigningKeyResponse;
import momento.sdk.messages.DeleteCacheResponse;
import momento.sdk.messages.FlushCacheResponse;
import momento.sdk.messages.ListCachesResponse;
import momento.sdk.messages.ListSigningKeysResponse;
import momento.sdk.messages.RevokeSigningKeyResponse;
import momento.sdk.messages.SigningKey;
import org.apache.commons.lang3.StringUtils;

/** Client for interacting with Scs Control Plane. */
final class ScsControlClient implements Closeable {

  private final ScsControlGrpcStubsManager controlGrpcStubsManager;

  ScsControlClient(String authToken, String endpoint) {
    this.controlGrpcStubsManager = new ScsControlGrpcStubsManager(authToken, endpoint);
  }

  CreateCacheResponse createCache(String cacheName) {
    try {
      checkCacheNameValid(cacheName);
      controlGrpcStubsManager.getBlockingStub().createCache(buildCreateCacheRequest(cacheName));
      return new CreateCacheResponse.Success();
    } catch (Exception e) {
      return new CreateCacheResponse.Error(CacheServiceExceptionMapper.convert(e));
    }
  }

  DeleteCacheResponse deleteCache(String cacheName) {
    try {
      checkCacheNameValid(cacheName);
      controlGrpcStubsManager.getBlockingStub().deleteCache(buildDeleteCacheRequest(cacheName));
      return new DeleteCacheResponse.Success();
    } catch (Exception e) {
      return new DeleteCacheResponse.Error(CacheServiceExceptionMapper.convert(e));
    }
  }

  FlushCacheResponse flushCache(String cacheName) {
    try {
      checkCacheNameValid(cacheName);
      controlGrpcStubsManager.getBlockingStub().flushCache(buildFlushCacheRequest(cacheName));
      return new FlushCacheResponse.Success();
    } catch (Exception e) {
      return new FlushCacheResponse.Error(CacheServiceExceptionMapper.convert(e));
    }
  }

  ListCachesResponse listCaches(Optional<String> nextToken) {
    try {
      _ListCachesRequest request =
          _ListCachesRequest.newBuilder().setNextToken(nextToken(nextToken)).build();
      return convert(controlGrpcStubsManager.getBlockingStub().listCaches(request));
    } catch (Exception e) {
      throw CacheServiceExceptionMapper.convert(e);
    }
  }

  CreateSigningKeyResponse createSigningKey(int ttlMinutes, String endpoint) {
    ensureValidTtlMinutes(ttlMinutes);
    try {
      return convert(
          controlGrpcStubsManager
              .getBlockingStub()
              .createSigningKey(buildCreateSigningKeyRequest(ttlMinutes)),
          endpoint);
    } catch (Exception e) {
      throw CacheServiceExceptionMapper.convert(e);
    }
  }

  RevokeSigningKeyResponse revokeSigningKey(String keyId) {
    try {
      controlGrpcStubsManager
          .getBlockingStub()
          .revokeSigningKey(buildRevokeSigningKeyRequest(keyId));
      return new RevokeSigningKeyResponse();
    } catch (Exception e) {
      throw CacheServiceExceptionMapper.convert(e);
    }
  }

  ListSigningKeysResponse listSigningKeys(Optional<String> nextToken, String endpoint) {
    try {
      _ListSigningKeysRequest request =
          _ListSigningKeysRequest.newBuilder().setNextToken(nextToken(nextToken)).build();
      return convert(controlGrpcStubsManager.getBlockingStub().listSigningKeys(request), endpoint);
    } catch (Exception e) {
      throw CacheServiceExceptionMapper.convert(e);
    }
  }

  private static _CreateCacheRequest buildCreateCacheRequest(String cacheName) {
    return _CreateCacheRequest.newBuilder().setCacheName(cacheName).build();
  }

  private static _DeleteCacheRequest buildDeleteCacheRequest(String cacheName) {
    return _DeleteCacheRequest.newBuilder().setCacheName(cacheName).build();
  }

  private static _FlushCacheRequest buildFlushCacheRequest(String cacheName) {
    return _FlushCacheRequest.newBuilder().setCacheName(cacheName).build();
  }

  private static _CreateSigningKeyRequest buildCreateSigningKeyRequest(int ttlMinutes) {
    return _CreateSigningKeyRequest.newBuilder().setTtlMinutes(ttlMinutes).build();
  }

  private static _RevokeSigningKeyRequest buildRevokeSigningKeyRequest(String keyId) {
    return _RevokeSigningKeyRequest.newBuilder().setKeyId(keyId).build();
  }

  private static ListCachesResponse convert(_ListCachesResponse response) {
    List<CacheInfo> caches = new ArrayList<>();
    for (_Cache cache : response.getCacheList()) {
      caches.add(convert(cache));
    }
    Optional<String> nextPageToken =
        StringUtils.isEmpty(response.getNextToken())
            ? Optional.empty()
            : Optional.of(response.getNextToken());
    return new ListCachesResponse(caches, nextPageToken);
  }

  private static String nextToken(Optional<String> nextToken) {
    return nextToken == null || !nextToken.isPresent() ? "" : nextToken.get();
  }

  private static CacheInfo convert(_Cache cache) {
    return new CacheInfo(cache.getCacheName());
  }

  private static ListSigningKeysResponse convert(
      _ListSigningKeysResponse response, String endpoint) {
    List<SigningKey> signingKeys = new ArrayList<>();
    for (_SigningKey signingKey : response.getSigningKeyList()) {
      signingKeys.add(convert(signingKey, endpoint));
    }
    Optional<String> nextPageToken =
        StringUtils.isEmpty(response.getNextToken())
            ? Optional.empty()
            : Optional.of(response.getNextToken());
    return new ListSigningKeysResponse(signingKeys, nextPageToken);
  }

  private static SigningKey convert(_SigningKey signingKey, String endpoint) {
    return new SigningKey(
        signingKey.getKeyId(), new Date(signingKey.getExpiresAt() * 1000), endpoint);
  }

  private static CreateSigningKeyResponse convert(
      _CreateSigningKeyResponse response, String endpoint) {
    JsonObject jsonObject = JsonParser.parseString(response.getKey()).getAsJsonObject();
    String keyId = jsonObject.get("kid").getAsString();
    return new CreateSigningKeyResponse(
        keyId, endpoint, response.getKey(), new Date(response.getExpiresAt() * 1000));
  }

  @Override
  public void close() {
    controlGrpcStubsManager.close();
  }
}
