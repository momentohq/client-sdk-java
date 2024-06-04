package momento.sdk;

import java.util.concurrent.CompletableFuture;
import momento.sdk.responses.persistent_store.control.CreatePersistentStoreResponse;
import momento.sdk.responses.persistent_store.control.DeletePersistentStoreResponse;
import momento.sdk.responses.persistent_store.control.ListPersistentStoresResponse;
import momento.sdk.responses.persistent_store.data.DeleteResponse;
import momento.sdk.responses.persistent_store.data.GetResponse;
import momento.sdk.responses.persistent_store.data.SetResponse;

public interface IPersistentStoreClient {
  /** Control operations */
  CompletableFuture<CreatePersistentStoreResponse> createStore(String storeName);

  CompletableFuture<DeletePersistentStoreResponse> deleteStore(String storeName);

  CompletableFuture<ListPersistentStoresResponse> listStores();

  /** Data operations */
  CompletableFuture<GetResponse> get(String storeName, String key);

  CompletableFuture<SetResponse> set(String storeName, String key, byte[] value);

  CompletableFuture<SetResponse> set(String storeName, String key, String value);

  CompletableFuture<SetResponse> set(String storeName, String key, long value);

  CompletableFuture<SetResponse> set(String storeName, String key, double value);

  CompletableFuture<DeleteResponse> delete(String storeName, String key);
}
