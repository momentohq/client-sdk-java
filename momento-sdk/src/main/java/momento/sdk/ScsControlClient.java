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
import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import javax.annotation.Nonnull;
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

/** Client for interacting with Scs Control Plane. */
final class ScsControlClient implements Closeable {

  private final ScsControlGrpcStubsManager controlGrpcStubsManager;

  ScsControlClient(@Nonnull String authToken, @Nonnull String endpoint) {
    this.controlGrpcStubsManager = new ScsControlGrpcStubsManager(authToken, endpoint);
  }

  CreateCacheResponse createCache(String cacheName) {
    try {
      checkCacheNameValid(cacheName);
      //noinspection ResultOfMethodCallIgnored

      controlGrpcStubsManager.getBlockingStub().createCache(buildCreateCacheRequest(cacheName));
      return new CreateCacheResponse.Success();
    } catch (Exception e) {
      return new CreateCacheResponse.Error(CacheServiceExceptionMapper.convert(e));
    }
  }

  DeleteCacheResponse deleteCache(String cacheName) {
    try {
      checkCacheNameValid(cacheName);
      //noinspection ResultOfMethodCallIgnored
      controlGrpcStubsManager.getBlockingStub().deleteCache(buildDeleteCacheRequest(cacheName));
      return new DeleteCacheResponse.Success();
    } catch (Exception e) {
      return new DeleteCacheResponse.Error(CacheServiceExceptionMapper.convert(e));
    }
  }

  FlushCacheResponse flushCache(String cacheName) {
    try {
      checkCacheNameValid(cacheName);
      //noinspection ResultOfMethodCallIgnored
      controlGrpcStubsManager.getBlockingStub().flushCache(buildFlushCacheRequest(cacheName));
      return new FlushCacheResponse.Success();
    } catch (Exception e) {
      return new FlushCacheResponse.Error(CacheServiceExceptionMapper.convert(e));
    }
  }

  ListCachesResponse listCaches() {
    try {
      final _ListCachesRequest request = _ListCachesRequest.newBuilder().setNextToken("").build();
      return convert(controlGrpcStubsManager.getBlockingStub().listCaches(request));
    } catch (Exception e) {
      return new ListCachesResponse.Error(CacheServiceExceptionMapper.convert(e));
    }
  }

  CreateSigningKeyResponse createSigningKey(Duration ttl, String endpoint) {
    try {
      ensureValidTtlMinutes(ttl);
      return convert(
          controlGrpcStubsManager
              .getBlockingStub()
              .createSigningKey(buildCreateSigningKeyRequest(ttl)),
          endpoint);
    } catch (Exception e) {
      return new CreateSigningKeyResponse.Error(CacheServiceExceptionMapper.convert(e));
    }
  }

  RevokeSigningKeyResponse revokeSigningKey(String keyId) {
    try {
      //noinspection ResultOfMethodCallIgnored
      controlGrpcStubsManager
          .getBlockingStub()
          .revokeSigningKey(buildRevokeSigningKeyRequest(keyId));
      return new RevokeSigningKeyResponse.Success();
    } catch (Exception e) {
      return new RevokeSigningKeyResponse.Error(CacheServiceExceptionMapper.convert(e));
    }
  }

  ListSigningKeysResponse listSigningKeys(String endpoint) {
    try {
      final _ListSigningKeysRequest request =
          _ListSigningKeysRequest.newBuilder().setNextToken("").build();
      return convert(controlGrpcStubsManager.getBlockingStub().listSigningKeys(request), endpoint);
    } catch (Exception e) {
      return new ListSigningKeysResponse.Error(CacheServiceExceptionMapper.convert(e));
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

  private static _CreateSigningKeyRequest buildCreateSigningKeyRequest(Duration ttl) {
    return _CreateSigningKeyRequest.newBuilder().setTtlMinutes((int) ttl.toMinutes()).build();
  }

  private static _RevokeSigningKeyRequest buildRevokeSigningKeyRequest(String keyId) {
    return _RevokeSigningKeyRequest.newBuilder().setKeyId(keyId).build();
  }

  private static ListCachesResponse convert(_ListCachesResponse response) {
    final List<CacheInfo> caches = new ArrayList<>();
    for (final _Cache cache : response.getCacheList()) {
      caches.add(convert(cache));
    }
    return new ListCachesResponse.Success(caches);
  }

  private static CacheInfo convert(_Cache cache) {
    return new CacheInfo(cache.getCacheName());
  }

  private static ListSigningKeysResponse convert(
      _ListSigningKeysResponse response, String endpoint) {
    final List<SigningKey> signingKeys = new ArrayList<>();
    for (final _SigningKey signingKey : response.getSigningKeyList()) {
      signingKeys.add(convert(signingKey, endpoint));
    }

    return new ListSigningKeysResponse.Success(signingKeys);
  }

  private static SigningKey convert(_SigningKey signingKey, String endpoint) {
    return new SigningKey(
        signingKey.getKeyId(), new Date(signingKey.getExpiresAt() * 1000), endpoint);
  }

  private static CreateSigningKeyResponse convert(
      _CreateSigningKeyResponse response, String endpoint) {
    final JsonObject jsonObject = JsonParser.parseString(response.getKey()).getAsJsonObject();
    final String keyId = jsonObject.get("kid").getAsString();
    return new CreateSigningKeyResponse.Success(
        keyId, endpoint, response.getKey(), new Date(response.getExpiresAt() * 1000));
  }

  @Override
  public void close() {
    controlGrpcStubsManager.close();
  }
}
