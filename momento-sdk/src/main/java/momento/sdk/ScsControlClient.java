package momento.sdk;

import static momento.sdk.ValidationUtils.checkCacheNameValid;
import static momento.sdk.ValidationUtils.ensureValidKey;
import static momento.sdk.ValidationUtils.ensureValidTtlMinutes;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import grpc.control_client._CreateCacheRequest;
import grpc.control_client._CreateCacheResponse;
import grpc.control_client._CreateSigningKeyRequest;
import grpc.control_client._CreateSigningKeyResponse;
import grpc.control_client._DeleteCacheRequest;
import grpc.control_client._DeleteCacheResponse;
import grpc.control_client._FlushCacheRequest;
import grpc.control_client._FlushCacheResponse;
import grpc.control_client._ListCachesRequest;
import grpc.control_client._ListCachesResponse;
import grpc.control_client._ListSigningKeysRequest;
import grpc.control_client._ListSigningKeysResponse;
import grpc.control_client._RevokeSigningKeyRequest;
import grpc.control_client._RevokeSigningKeyResponse;
import io.grpc.Metadata;
import java.time.Duration;
import java.util.Date;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import momento.sdk.auth.CredentialProvider;
import momento.sdk.config.Configuration;
import momento.sdk.exceptions.CacheServiceExceptionMapper;
import momento.sdk.exceptions.InternalServerException;
import momento.sdk.responses.cache.control.CacheCreateResponse;
import momento.sdk.responses.cache.control.CacheDeleteResponse;
import momento.sdk.responses.cache.control.CacheFlushResponse;
import momento.sdk.responses.cache.control.CacheInfo;
import momento.sdk.responses.cache.control.CacheListResponse;
import momento.sdk.responses.cache.signing.SigningKey;
import momento.sdk.responses.cache.signing.SigningKeyCreateResponse;
import momento.sdk.responses.cache.signing.SigningKeyListResponse;
import momento.sdk.responses.cache.signing.SigningKeyRevokeResponse;

/** Client for interacting with Scs Control Plane. */
final class ScsControlClient extends ScsClientBase {

  private final CredentialProvider credentialProvider;
  private final ScsControlGrpcStubsManager controlGrpcStubsManager;

  ScsControlClient(@Nonnull CredentialProvider credentialProvider, Configuration configuration) {
    this.credentialProvider = credentialProvider;
    this.controlGrpcStubsManager =
        new ScsControlGrpcStubsManager(credentialProvider, configuration);
  }

  CompletableFuture<CacheCreateResponse> createCache(String cacheName) {
    try {
      checkCacheNameValid(cacheName);

      return sendCreateCache(cacheName);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheCreateResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheDeleteResponse> deleteCache(String cacheName) {
    try {
      checkCacheNameValid(cacheName);

      return sendDeleteCache(cacheName);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheDeleteResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheFlushResponse> flushCache(String cacheName) {
    try {
      checkCacheNameValid(cacheName);

      return sendFlushCache(cacheName);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheFlushResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheListResponse> listCaches() {
    try {
      return sendListCaches();
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheListResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<SigningKeyCreateResponse> createSigningKey(Duration ttl) {
    try {
      ensureValidTtlMinutes(ttl);

      return sendCreateSigningKey(ttl);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new SigningKeyCreateResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<SigningKeyRevokeResponse> revokeSigningKey(String keyId) {
    try {
      ensureValidKey(keyId);

      return sendRevokeSigningKey(keyId);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new SigningKeyRevokeResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<SigningKeyListResponse> listSigningKeys() {
    try {
      return sendListSigningKeys();
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new SigningKeyListResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  private CompletableFuture<CacheCreateResponse> sendCreateCache(String cacheName) {
    final Metadata metadata = metadataWithCache(cacheName);

    final Supplier<ListenableFuture<_CreateCacheResponse>> stubSupplier =
        () ->
            attachMetadata(controlGrpcStubsManager.getStub(), metadata)
                .createCache(_CreateCacheRequest.newBuilder().setCacheName(cacheName).build());

    final Function<_CreateCacheResponse, CacheCreateResponse> success =
        rsp -> new CacheCreateResponse.Success();

    final Function<Throwable, CacheCreateResponse> failure =
        e -> new CacheCreateResponse.Error(CacheServiceExceptionMapper.convert(e, metadata));

    return executeGrpcFunction(stubSupplier, success, failure);
  }

  private CompletableFuture<CacheDeleteResponse> sendDeleteCache(String cacheName) {
    final Metadata metadata = metadataWithCache(cacheName);

    final Supplier<ListenableFuture<_DeleteCacheResponse>> stubSupplier =
        () ->
            attachMetadata(controlGrpcStubsManager.getStub(), metadata)
                .deleteCache(_DeleteCacheRequest.newBuilder().setCacheName(cacheName).build());

    final Function<_DeleteCacheResponse, CacheDeleteResponse> success =
        rsp -> new CacheDeleteResponse.Success();

    final Function<Throwable, CacheDeleteResponse> failure =
        e -> new CacheDeleteResponse.Error(CacheServiceExceptionMapper.convert(e, metadata));

    return executeGrpcFunction(stubSupplier, success, failure);
  }

  private CompletableFuture<CacheFlushResponse> sendFlushCache(String cacheName) {
    final Metadata metadata = metadataWithCache(cacheName);

    final Supplier<ListenableFuture<_FlushCacheResponse>> stubSupplier =
        () ->
            attachMetadata(controlGrpcStubsManager.getStub(), metadata)
                .flushCache(_FlushCacheRequest.newBuilder().setCacheName(cacheName).build());

    final Function<_FlushCacheResponse, CacheFlushResponse> success =
        rsp -> new CacheFlushResponse.Success();

    final Function<Throwable, CacheFlushResponse> failure =
        e -> new CacheFlushResponse.Error(CacheServiceExceptionMapper.convert(e, metadata));

    return executeGrpcFunction(stubSupplier, success, failure);
  }

  private CompletableFuture<CacheListResponse> sendListCaches() {

    final Supplier<ListenableFuture<_ListCachesResponse>> stubSupplier =
        () ->
            controlGrpcStubsManager
                .getStub()
                .listCaches(_ListCachesRequest.newBuilder().setNextToken("").build());

    final Function<_ListCachesResponse, CacheListResponse> success =
        rsp ->
            new CacheListResponse.Success(
                rsp.getCacheList().stream()
                    .map(c -> new CacheInfo(c.getCacheName()))
                    .collect(Collectors.toList()));

    final Function<Throwable, CacheListResponse> failure =
        e -> new CacheListResponse.Error(CacheServiceExceptionMapper.convert(e));

    return executeGrpcFunction(stubSupplier, success, failure);
  }

  private CompletableFuture<SigningKeyCreateResponse> sendCreateSigningKey(Duration ttl) {

    final Supplier<ListenableFuture<_CreateSigningKeyResponse>> stubSupplier =
        () ->
            controlGrpcStubsManager
                .getStub()
                .createSigningKey(
                    _CreateSigningKeyRequest.newBuilder()
                        .setTtlMinutes((int) ttl.toMinutes())
                        .build());

    final Function<_CreateSigningKeyResponse, SigningKeyCreateResponse> success =
        rsp -> {
          try {
            final JsonObject jsonObject = JsonParser.parseString(rsp.getKey()).getAsJsonObject();
            final String keyId = jsonObject.get("kid").getAsString();
            return new SigningKeyCreateResponse.Success(
                keyId,
                credentialProvider.getCacheEndpoint(),
                rsp.getKey(),
                new Date(rsp.getExpiresAt() * 1000));
          } catch (Exception e) {
            return new SigningKeyCreateResponse.Error(
                new InternalServerException(
                    "Unable to parse key ID from server response. Please contact Momento."));
          }
        };

    final Function<Throwable, SigningKeyCreateResponse> failure =
        e -> new SigningKeyCreateResponse.Error(CacheServiceExceptionMapper.convert(e));

    return executeGrpcFunction(stubSupplier, success, failure);
  }

  private CompletableFuture<SigningKeyRevokeResponse> sendRevokeSigningKey(String keyId) {

    final Supplier<ListenableFuture<_RevokeSigningKeyResponse>> stubSupplier =
        () ->
            controlGrpcStubsManager
                .getStub()
                .revokeSigningKey(_RevokeSigningKeyRequest.newBuilder().setKeyId(keyId).build());

    final Function<_RevokeSigningKeyResponse, SigningKeyRevokeResponse> success =
        rsp -> new SigningKeyRevokeResponse.Success();

    final Function<Throwable, SigningKeyRevokeResponse> failure =
        e -> new SigningKeyRevokeResponse.Error(CacheServiceExceptionMapper.convert(e));

    return executeGrpcFunction(stubSupplier, success, failure);
  }

  private CompletableFuture<SigningKeyListResponse> sendListSigningKeys() {

    final Supplier<ListenableFuture<_ListSigningKeysResponse>> stubSupplier =
        () ->
            controlGrpcStubsManager
                .getStub()
                .listSigningKeys(_ListSigningKeysRequest.newBuilder().setNextToken("").build());

    final Function<_ListSigningKeysResponse, SigningKeyListResponse> success =
        rsp ->
            new SigningKeyListResponse.Success(
                rsp.getSigningKeyList().stream()
                    .map(
                        sk ->
                            new SigningKey(
                                sk.getKeyId(),
                                new Date(sk.getExpiresAt() * 1000),
                                credentialProvider.getCacheEndpoint()))
                    .collect(Collectors.toList()));

    final Function<Throwable, SigningKeyListResponse> failure =
        e -> new SigningKeyListResponse.Error(CacheServiceExceptionMapper.convert(e));

    return executeGrpcFunction(stubSupplier, success, failure);
  }

  @Override
  public void close() {
    controlGrpcStubsManager.close();
  }
}
