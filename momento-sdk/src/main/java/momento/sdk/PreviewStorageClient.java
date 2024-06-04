package momento.sdk;

import java.util.concurrent.CompletableFuture;
import momento.sdk.exceptions.NotFoundException;
import momento.sdk.responses.storage.control.CreatePersistentStoreResponse;
import momento.sdk.responses.storage.control.DeletePersistentStoreResponse;
import momento.sdk.responses.storage.control.ListPersistentStoresResponse;
import momento.sdk.responses.storage.data.DeleteResponse;
import momento.sdk.responses.storage.data.GetResponse;
import momento.sdk.responses.storage.data.SetResponse;

public class PreviewStorageClient implements IPreviewStorageClient {
  /** Control operations */
  public CompletableFuture<CreatePersistentStoreResponse> createStore(String storeName) {
    return CompletableFuture.completedFuture(new CreatePersistentStoreResponse.Success());
  }

  public CompletableFuture<DeletePersistentStoreResponse> deleteStore(String storeName) {
    return CompletableFuture.completedFuture(new DeletePersistentStoreResponse.Success());
  }

  public CompletableFuture<ListPersistentStoresResponse> listStores() {

    return CompletableFuture.completedFuture(new ListPersistentStoresResponse.Success());
  }

  /** Data operations */
  public CompletableFuture<GetResponse> get(String storeName, String key) {
    if (key.equalsIgnoreCase("byte array")) {
      return CompletableFuture.completedFuture(GetResponse.Success.of(new byte[0]));
    } else if (key.equalsIgnoreCase("string")) {
      return CompletableFuture.completedFuture(GetResponse.Success.of("string"));
    } else if (key.equalsIgnoreCase("long")) {
      return CompletableFuture.completedFuture(GetResponse.Success.of(42L));
    } else if (key.equalsIgnoreCase("double")) {
      return CompletableFuture.completedFuture(GetResponse.Success.of(3.14));
    } else {
      return CompletableFuture.completedFuture(
          new GetResponse.Error(new NotFoundException(new Exception("Key not found"), null)));
    }
  }

  public CompletableFuture<SetResponse> set(String storeName, String key, byte[] value) {
    return CompletableFuture.completedFuture(new SetResponse.Success());
  }

  public CompletableFuture<SetResponse> set(String storeName, String key, String value) {
    return CompletableFuture.completedFuture(new SetResponse.Success());
  }

  public CompletableFuture<SetResponse> set(String storeName, String key, long value) {
    return CompletableFuture.completedFuture(new SetResponse.Success());
  }

  public CompletableFuture<SetResponse> set(String storeName, String key, double value) {
    return CompletableFuture.completedFuture(new SetResponse.Success());
  }

  public CompletableFuture<DeleteResponse> delete(String storeName, String key) {
    return CompletableFuture.completedFuture(new DeleteResponse.Success());
  }
}
