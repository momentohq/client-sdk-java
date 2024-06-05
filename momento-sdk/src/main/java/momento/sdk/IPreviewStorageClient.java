package momento.sdk;

import java.util.concurrent.CompletableFuture;
import momento.sdk.responses.storage.control.CreateStoreResponse;
import momento.sdk.responses.storage.control.DeleteStoreResponse;
import momento.sdk.responses.storage.control.ListStoresResponse;
import momento.sdk.responses.storage.data.DeleteResponse;
import momento.sdk.responses.storage.data.GetResponse;
import momento.sdk.responses.storage.data.SetResponse;

public interface IPreviewStorageClient {
  /** Control operations */
  CompletableFuture<CreateStoreResponse> createStore(String storeName);

  CompletableFuture<DeleteStoreResponse> deleteStore(String storeName);

  CompletableFuture<ListStoresResponse> listStores();

  /** Data operations */
  CompletableFuture<GetResponse> get(String storeName, String key);

  CompletableFuture<SetResponse> set(String storeName, String key, byte[] value);

  CompletableFuture<SetResponse> set(String storeName, String key, String value);

  CompletableFuture<SetResponse> set(String storeName, String key, long value);

  CompletableFuture<SetResponse> set(String storeName, String key, double value);

  CompletableFuture<DeleteResponse> delete(String storeName, String key);
}
