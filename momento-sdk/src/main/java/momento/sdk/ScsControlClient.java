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
import momento.sdk.exceptions.CacheServiceExceptionMapper;
import momento.sdk.exceptions.InternalServerException;
import momento.sdk.responses.CacheInfo;
import momento.sdk.responses.CreateCacheResponse;
import momento.sdk.responses.CreateSigningKeyResponse;
import momento.sdk.responses.DeleteCacheResponse;
import momento.sdk.responses.FlushCacheResponse;
import momento.sdk.responses.ListCachesResponse;
import momento.sdk.responses.ListSigningKeysResponse;
import momento.sdk.responses.RevokeSigningKeyResponse;
import momento.sdk.responses.SigningKey;

/** Client for interacting with Scs Control Plane. */
final class ScsControlClient extends ScsClient {

  private final CredentialProvider credentialProvider;
  private final ScsControlGrpcStubsManager controlGrpcStubsManager;

  ScsControlClient(@Nonnull CredentialProvider credentialProvider) {
    this.credentialProvider = credentialProvider;
    this.controlGrpcStubsManager = new ScsControlGrpcStubsManager(credentialProvider);
  }

  CompletableFuture<CreateCacheResponse> createCache(String cacheName) {
    try {
      checkCacheNameValid(cacheName);

      return sendCreateCache(cacheName);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CreateCacheResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<DeleteCacheResponse> deleteCache(String cacheName) {
    try {
      checkCacheNameValid(cacheName);

      return sendDeleteCache(cacheName);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new DeleteCacheResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<FlushCacheResponse> flushCache(String cacheName) {
    try {
      checkCacheNameValid(cacheName);

      return sendFlushCache(cacheName);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new FlushCacheResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<ListCachesResponse> listCaches() {
    try {
      return sendListCaches();
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new ListCachesResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CreateSigningKeyResponse> createSigningKey(Duration ttl) {
    try {
      ensureValidTtlMinutes(ttl);

      return sendCreateSigningKey(ttl);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CreateSigningKeyResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<RevokeSigningKeyResponse> revokeSigningKey(String keyId) {
    try {
      ensureValidKey(keyId);

      return sendRevokeSigningKey(keyId);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new RevokeSigningKeyResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<ListSigningKeysResponse> listSigningKeys() {
    try {
      return sendListSigningKeys();
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new ListSigningKeysResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  private CompletableFuture<CreateCacheResponse> sendCreateCache(String cacheName) {
    final Metadata metadata = metadataWithCache(cacheName);

    final Supplier<ListenableFuture<_CreateCacheResponse>> stubSupplier =
        () ->
            attachMetadata(controlGrpcStubsManager.getStub(), metadata)
                .createCache(_CreateCacheRequest.newBuilder().setCacheName(cacheName).build());

    final Function<_CreateCacheResponse, CreateCacheResponse> success =
        rsp -> new CreateCacheResponse.Success();

    final Function<Throwable, CreateCacheResponse> failure =
        e -> new CreateCacheResponse.Error(CacheServiceExceptionMapper.convert(e, metadata));

    return executeGrpcFunction(stubSupplier, success, failure);
  }

  private CompletableFuture<DeleteCacheResponse> sendDeleteCache(String cacheName) {
    final Metadata metadata = metadataWithCache(cacheName);

    final Supplier<ListenableFuture<_DeleteCacheResponse>> stubSupplier =
        () ->
            attachMetadata(controlGrpcStubsManager.getStub(), metadata)
                .deleteCache(_DeleteCacheRequest.newBuilder().setCacheName(cacheName).build());

    final Function<_DeleteCacheResponse, DeleteCacheResponse> success =
        rsp -> new DeleteCacheResponse.Success();

    final Function<Throwable, DeleteCacheResponse> failure =
        e -> new DeleteCacheResponse.Error(CacheServiceExceptionMapper.convert(e, metadata));

    return executeGrpcFunction(stubSupplier, success, failure);
  }

  private CompletableFuture<FlushCacheResponse> sendFlushCache(String cacheName) {
    final Metadata metadata = metadataWithCache(cacheName);

    final Supplier<ListenableFuture<_FlushCacheResponse>> stubSupplier =
        () ->
            attachMetadata(controlGrpcStubsManager.getStub(), metadata)
                .flushCache(_FlushCacheRequest.newBuilder().setCacheName(cacheName).build());

    final Function<_FlushCacheResponse, FlushCacheResponse> success =
        rsp -> new FlushCacheResponse.Success();

    final Function<Throwable, FlushCacheResponse> failure =
        e -> new FlushCacheResponse.Error(CacheServiceExceptionMapper.convert(e, metadata));

    return executeGrpcFunction(stubSupplier, success, failure);
  }

  private CompletableFuture<ListCachesResponse> sendListCaches() {

    final Supplier<ListenableFuture<_ListCachesResponse>> stubSupplier =
        () ->
            controlGrpcStubsManager
                .getStub()
                .listCaches(_ListCachesRequest.newBuilder().setNextToken("").build());

    final Function<_ListCachesResponse, ListCachesResponse> success =
        rsp ->
            new ListCachesResponse.Success(
                rsp.getCacheList().stream()
                    .map(c -> new CacheInfo(c.getCacheName()))
                    .collect(Collectors.toList()));

    final Function<Throwable, ListCachesResponse> failure =
        e -> new ListCachesResponse.Error(CacheServiceExceptionMapper.convert(e));

    return executeGrpcFunction(stubSupplier, success, failure);
  }

  private CompletableFuture<CreateSigningKeyResponse> sendCreateSigningKey(Duration ttl) {

    final Supplier<ListenableFuture<_CreateSigningKeyResponse>> stubSupplier =
        () ->
            controlGrpcStubsManager
                .getStub()
                .createSigningKey(
                    _CreateSigningKeyRequest.newBuilder()
                        .setTtlMinutes((int) ttl.toMinutes())
                        .build());

    final Function<_CreateSigningKeyResponse, CreateSigningKeyResponse> success =
        rsp -> {
          try {
            final JsonObject jsonObject = JsonParser.parseString(rsp.getKey()).getAsJsonObject();
            final String keyId = jsonObject.get("kid").getAsString();
            return new CreateSigningKeyResponse.Success(
                keyId,
                credentialProvider.getCacheEndpoint(),
                rsp.getKey(),
                new Date(rsp.getExpiresAt() * 1000));
          } catch (Exception e) {
            return new CreateSigningKeyResponse.Error(
                new InternalServerException(
                    "Unable to parse key ID from server response. Please contact Momento."));
          }
        };

    final Function<Throwable, CreateSigningKeyResponse> failure =
        e -> new CreateSigningKeyResponse.Error(CacheServiceExceptionMapper.convert(e));

    return executeGrpcFunction(stubSupplier, success, failure);
  }

  private CompletableFuture<RevokeSigningKeyResponse> sendRevokeSigningKey(String keyId) {

    final Supplier<ListenableFuture<_RevokeSigningKeyResponse>> stubSupplier =
        () ->
            controlGrpcStubsManager
                .getStub()
                .revokeSigningKey(_RevokeSigningKeyRequest.newBuilder().setKeyId(keyId).build());

    final Function<_RevokeSigningKeyResponse, RevokeSigningKeyResponse> success =
        rsp -> new RevokeSigningKeyResponse.Success();

    final Function<Throwable, RevokeSigningKeyResponse> failure =
        e -> new RevokeSigningKeyResponse.Error(CacheServiceExceptionMapper.convert(e));

    return executeGrpcFunction(stubSupplier, success, failure);
  }

  private CompletableFuture<ListSigningKeysResponse> sendListSigningKeys() {

    final Supplier<ListenableFuture<_ListSigningKeysResponse>> stubSupplier =
        () ->
            controlGrpcStubsManager
                .getStub()
                .listSigningKeys(_ListSigningKeysRequest.newBuilder().setNextToken("").build());

    final Function<_ListSigningKeysResponse, ListSigningKeysResponse> success =
        rsp ->
            new ListSigningKeysResponse.Success(
                rsp.getSigningKeyList().stream()
                    .map(
                        sk ->
                            new SigningKey(
                                sk.getKeyId(),
                                new Date(sk.getExpiresAt() * 1000),
                                credentialProvider.getCacheEndpoint()))
                    .collect(Collectors.toList()));

    final Function<Throwable, ListSigningKeysResponse> failure =
        e -> new ListSigningKeysResponse.Error(CacheServiceExceptionMapper.convert(e));

    return executeGrpcFunction(stubSupplier, success, failure);
  }

  @Override
  public void close() {
    controlGrpcStubsManager.close();
  }
}
