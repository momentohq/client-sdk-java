package momento.sdk;

import static momento.sdk.ValidationUtils.checkStoreNameValid;

import com.google.common.util.concurrent.ListenableFuture;
import grpc.control_client._CreateStoreRequest;
import grpc.control_client._CreateStoreResponse;
import grpc.control_client._DeleteStoreRequest;
import grpc.control_client._DeleteStoreResponse;
import grpc.control_client._ListStoresRequest;
import grpc.control_client._ListStoresResponse;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import momento.sdk.auth.CredentialProvider;
import momento.sdk.config.StorageConfiguration;
import momento.sdk.exceptions.CacheServiceExceptionMapper;
import momento.sdk.responses.storage.control.CreateStoreResponse;
import momento.sdk.responses.storage.control.DeleteStoreResponse;
import momento.sdk.responses.storage.control.ListStoresResponse;
import momento.sdk.responses.storage.control.StoreInfo;

/** Client for interacting with Scs Control Plane. */
final class StorageControlClient extends ScsClientBase {

  private final CredentialProvider credentialProvider;
  private final StorageControlGrpcStubsManager controlGrpcStubsManager;

  StorageControlClient(
      @Nonnull CredentialProvider credentialProvider, StorageConfiguration configuration) {
    this.credentialProvider = credentialProvider;
    this.controlGrpcStubsManager =
        new StorageControlGrpcStubsManager(credentialProvider, configuration);
  }

  CompletableFuture<CreateStoreResponse> createCache(String storeName) {
    try {
      checkStoreNameValid(storeName);

      return sendCreateStore(storeName);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          // TODO need to generalize exception mapper
          new CreateStoreResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<DeleteStoreResponse> deleteCache(String cacheName) {
    try {
      checkStoreNameValid(cacheName);

      return sendDeleteCache(cacheName);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new DeleteStoreResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<ListStoresResponse> listCaches() {
    try {
      return sendListCaches();
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new ListStoresResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  private CompletableFuture<CreateStoreResponse> sendCreateStore(String storeName) {
    final Supplier<ListenableFuture<_CreateStoreResponse>> stubSupplier =
        () ->
            controlGrpcStubsManager
                .getStub()
                .createStore(_CreateStoreRequest.newBuilder().setStoreName(storeName).build());

    final Function<_CreateStoreResponse, CreateStoreResponse> success =
        rsp -> new CreateStoreResponse.Success();

    final Function<Throwable, CreateStoreResponse> failure =
        e -> new CreateStoreResponse.Error(CacheServiceExceptionMapper.convert(e));

    return executeGrpcFunction(stubSupplier, success, failure);
  }

  private CompletableFuture<DeleteStoreResponse> sendDeleteCache(String storeName) {
    final Supplier<ListenableFuture<_DeleteStoreResponse>> stubSupplier =
        () ->
            controlGrpcStubsManager
                .getStub()
                .deleteStore(_DeleteStoreRequest.newBuilder().setStoreName(storeName).build());

    final Function<_DeleteStoreResponse, DeleteStoreResponse> success =
        rsp -> new DeleteStoreResponse.Success();

    final Function<Throwable, DeleteStoreResponse> failure =
        e -> new DeleteStoreResponse.Error(CacheServiceExceptionMapper.convert(e));

    return executeGrpcFunction(stubSupplier, success, failure);
  }

  private CompletableFuture<ListStoresResponse> sendListCaches() {

    final Supplier<ListenableFuture<_ListStoresResponse>> stubSupplier =
        () ->
            controlGrpcStubsManager
                .getStub()
                .listStores(_ListStoresRequest.newBuilder().setNextToken("").build());

    final Function<_ListStoresResponse, ListStoresResponse> success =
        rsp ->
            new ListStoresResponse.Success(
                rsp.getStoreList().stream()
                    .map(s -> new StoreInfo(s.getStoreName()))
                    .collect(Collectors.toList()));

    final Function<Throwable, ListStoresResponse> failure =
        e -> new ListStoresResponse.Error(CacheServiceExceptionMapper.convert(e));

    return executeGrpcFunction(stubSupplier, success, failure);
  }

  @Override
  public void close() {
    controlGrpcStubsManager.close();
  }
}
