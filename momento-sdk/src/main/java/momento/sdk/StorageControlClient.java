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
import momento.sdk.exceptions.SdkException;
import momento.sdk.exceptions.StoreAlreadyExistsException;
import momento.sdk.responses.storage.CreateStoreResponse;
import momento.sdk.responses.storage.DeleteStoreResponse;
import momento.sdk.responses.storage.ListStoresResponse;
import momento.sdk.responses.storage.StoreInfo;

/** Client for interacting with Scs Control Plane. */
final class StorageControlClient extends ScsClientBase {

  private final CredentialProvider credentialProvider;
  private final StorageControlGrpcStubsManager controlGrpcStubsManager;

  StorageControlClient(
      @Nonnull CredentialProvider credentialProvider, StorageConfiguration configuration) {
    super(null);
    this.credentialProvider = credentialProvider;
    this.controlGrpcStubsManager =
        new StorageControlGrpcStubsManager(credentialProvider, configuration);
  }

  CompletableFuture<CreateStoreResponse> createStore(String storeName) {
    try {
      checkStoreNameValid(storeName);

      return sendCreateStore(storeName);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CreateStoreResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<DeleteStoreResponse> deleteStore(String cacheName) {
    try {
      checkStoreNameValid(cacheName);

      return sendDeleteStore(cacheName);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new DeleteStoreResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<ListStoresResponse> listStores() {
    try {
      return sendListStores();
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
        e -> {
          final SdkException sdkException = CacheServiceExceptionMapper.convert(e);
          if (sdkException instanceof StoreAlreadyExistsException) {
            return new CreateStoreResponse.AlreadyExists();
          }
          return new CreateStoreResponse.Error(CacheServiceExceptionMapper.convert(e));
        };

    return executeGrpcFunction(stubSupplier, success, failure);
  }

  private CompletableFuture<DeleteStoreResponse> sendDeleteStore(String storeName) {
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

  private CompletableFuture<ListStoresResponse> sendListStores() {

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
  public void doClose() {
    controlGrpcStubsManager.close();
  }
}
