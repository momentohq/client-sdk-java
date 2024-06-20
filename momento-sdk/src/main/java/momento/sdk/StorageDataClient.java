package momento.sdk;

import static momento.sdk.ValidationUtils.checkCacheNameValid;
import static momento.sdk.ValidationUtils.ensureValidKey;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import grpc.store._StoreDeleteRequest;
import grpc.store._StoreDeleteResponse;
import grpc.store._StoreGetRequest;
import grpc.store._StoreGetResponse;
import grpc.store._StorePutRequest;
import grpc.store._StorePutResponse;
import grpc.store._StoreValue;
import io.grpc.Metadata;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import momento.sdk.auth.CredentialProvider;
import momento.sdk.config.StorageConfiguration;
import momento.sdk.exceptions.CacheServiceExceptionMapper;
import momento.sdk.exceptions.InternalServerException;
import momento.sdk.exceptions.SdkException;
import momento.sdk.responses.storage.DeleteResponse;
import momento.sdk.responses.storage.GetResponse;
import momento.sdk.responses.storage.PutResponse;

/** Client for interacting with Scs Data plane. */
final class StorageDataClient extends StorageClientBase {
  private final StorageDataGrpcStubsManager storageDataGrpcStubsManager;

  StorageDataClient(
      @Nonnull CredentialProvider credentialProvider, @Nonnull StorageConfiguration configuration) {
    this.storageDataGrpcStubsManager =
        new StorageDataGrpcStubsManager(credentialProvider, configuration);
  }

  public void connect(final long eagerConnectionTimeout) {
    this.storageDataGrpcStubsManager.connect(eagerConnectionTimeout);
  }

  CompletableFuture<GetResponse> get(String storeName, String key) {
    try {
      ensureValidKey(key);
      return sendGet(storeName, key);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new GetResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<PutResponse> put(String storeName, String key, byte[] value) {
    try {
      ensureValidKey(key);
      _StoreValue storeValue =
          _StoreValue.newBuilder().setBytesValue(ByteString.copyFrom(value)).build();
      return sendPut(storeName, key, storeValue);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new PutResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<PutResponse> put(String storeName, String key, String value) {
    try {
      ensureValidKey(key);
      _StoreValue storeValue = _StoreValue.newBuilder().setStringValue(value).build();
      return sendPut(storeName, key, storeValue);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new PutResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<PutResponse> put(String storeName, String key, long value) {
    try {
      ensureValidKey(key);
      _StoreValue storeValue = _StoreValue.newBuilder().setIntegerValue(value).build();
      return sendPut(storeName, key, storeValue);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new PutResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<PutResponse> put(String storeName, String key, double value) {
    try {
      ensureValidKey(key);
      _StoreValue storeValue = _StoreValue.newBuilder().setDoubleValue(value).build();
      return sendPut(storeName, key, storeValue);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new PutResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<DeleteResponse> delete(String storeName, String key) {
    try {
      ensureValidKey(key);
      return sendDelete(storeName, key);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new DeleteResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  private CompletableFuture<GetResponse> sendGet(String storeName, String key) {
    checkCacheNameValid(storeName);

    // Submit request to non-blocking stub
    final Metadata metadata = metadataWithStore(storeName);
    final ListenableFuture<_StoreGetResponse> rspFuture =
        attachMetadata(storageDataGrpcStubsManager.getStub(), metadata)
            .get(_StoreGetRequest.newBuilder().setKey(key).build());

    // Build a CompletableFuture to return to caller
    final CompletableFuture<GetResponse> returnFuture =
        new CompletableFuture<GetResponse>() {
          @Override
          public boolean cancel(boolean mayInterruptIfRunning) {
            // propagate cancel to the listenable future if called on returned completable future
            final boolean result = rspFuture.cancel(mayInterruptIfRunning);
            super.cancel(mayInterruptIfRunning);
            return result;
          }
        };

    // Convert returned ListenableFuture to CompletableFuture
    Futures.addCallback(
        rspFuture,
        new FutureCallback<_StoreGetResponse>() {
          @Override
          public void onSuccess(_StoreGetResponse rsp) {
            _StoreValue value = rsp.getValue();
            switch (value.getValueCase()) {
              case BYTES_VALUE:
                returnFuture.complete(GetResponse.Success.of(value.getBytesValue().toByteArray()));
                break;
              case STRING_VALUE:
                returnFuture.complete(GetResponse.Success.of(value.getStringValue()));
                break;
              case INTEGER_VALUE:
                returnFuture.complete(GetResponse.Success.of(value.getIntegerValue()));
                break;
              case DOUBLE_VALUE:
                returnFuture.complete(GetResponse.Success.of(value.getDoubleValue()));
                break;
              case VALUE_NOT_SET:
                returnFuture.complete(
                    new GetResponse.Error(
                        new InternalServerException(
                            "Unsupported cache Get result: " + value.getValueCase())));
            }
          }

          @Override
          public void onFailure(@Nonnull Throwable e) {
            final SdkException sdkException = CacheServiceExceptionMapper.convert(e, metadata);
            if (sdkException instanceof momento.sdk.exceptions.StoreItemNotFoundException) {
              returnFuture.complete(GetResponse.Success.of());
              return;
            }
            returnFuture.complete(
                new GetResponse.Error(CacheServiceExceptionMapper.convert(e, metadata)));
          }
        },
        // Execute on same thread that called execute on CompletionStage
        MoreExecutors.directExecutor());

    return returnFuture;
  }

  private CompletableFuture<PutResponse> sendPut(String storeName, String key, _StoreValue value) {
    checkCacheNameValid(storeName);
    final Metadata metadata = metadataWithStore(storeName);

    // Submit request to non-blocking stub
    final ListenableFuture<_StorePutResponse> rspFuture =
        attachMetadata(storageDataGrpcStubsManager.getStub(), metadata)
            .put(_StorePutRequest.newBuilder().setKey(key).setValue(value).build());

    // Build a CompletableFuture to return to caller
    final CompletableFuture<PutResponse> returnFuture =
        new CompletableFuture<PutResponse>() {
          @Override
          public boolean cancel(boolean mayInterruptIfRunning) {
            // propagate cancel to the listenable future if called on returned completable future
            final boolean result = rspFuture.cancel(mayInterruptIfRunning);
            super.cancel(mayInterruptIfRunning);
            return result;
          }
        };

    // Convert returned ListenableFuture to CompletableFuture
    Futures.addCallback(
        rspFuture,
        new FutureCallback<_StorePutResponse>() {
          @Override
          public void onSuccess(_StorePutResponse rsp) {
            returnFuture.complete(new PutResponse.Success());
          }

          @Override
          public void onFailure(@Nonnull Throwable e) {
            returnFuture.complete(
                new PutResponse.Error(CacheServiceExceptionMapper.convert(e, new Metadata())));
          }
        },
        // Execute on same thread that called execute on CompletionStage
        MoreExecutors.directExecutor());

    return returnFuture;
  }

  private CompletableFuture<DeleteResponse> sendDelete(String storeName, String key) {
    checkCacheNameValid(storeName);
    // Submit request to non-blocking stub
    final Metadata metadata = metadataWithStore(storeName);
    final ListenableFuture<_StoreDeleteResponse> rspFuture =
        attachMetadata(storageDataGrpcStubsManager.getStub(), metadata)
            .delete(_StoreDeleteRequest.newBuilder().setKey(key).build());

    // Build a CompletableFuture to return to caller
    final CompletableFuture<DeleteResponse> returnFuture =
        new CompletableFuture<DeleteResponse>() {
          @Override
          public boolean cancel(boolean mayInterruptIfRunning) {
            // propagate cancel to the listenable future if called on returned completable future
            final boolean result = rspFuture.cancel(mayInterruptIfRunning);
            super.cancel(mayInterruptIfRunning);
            return result;
          }
        };

    // Convert returned ListenableFuture to CompletableFuture
    Futures.addCallback(
        rspFuture,
        new FutureCallback<_StoreDeleteResponse>() {
          @Override
          public void onSuccess(_StoreDeleteResponse rsp) {
            returnFuture.complete(new DeleteResponse.Success());
          }

          @Override
          public void onFailure(@Nonnull Throwable e) {
            returnFuture.complete(
                new DeleteResponse.Error(CacheServiceExceptionMapper.convert(e, metadata)));
          }
        },
        // Execute on same thread that called execute on CompletionStage
        MoreExecutors.directExecutor());

    return returnFuture;
  }

  @Override
  public void close() {
    storageDataGrpcStubsManager.close();
  }
}
