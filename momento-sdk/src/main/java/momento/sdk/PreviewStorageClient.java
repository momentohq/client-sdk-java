package momento.sdk;

import java.util.concurrent.CompletableFuture;
import momento.sdk.exceptions.NotFoundException;
import momento.sdk.responses.storage.control.CreateStoreResponse;
import momento.sdk.responses.storage.control.DeleteStoreResponse;
import momento.sdk.responses.storage.control.ListStoresResponse;
import momento.sdk.responses.storage.data.DeleteResponse;
import momento.sdk.responses.storage.data.GetResponse;
import momento.sdk.responses.storage.data.SetResponse;

public class PreviewStorageClient implements IPreviewStorageClient, AutoCloseable {
  /** Control operations */
  public CompletableFuture<CreateStoreResponse> createStore(String storeName) {
    return CompletableFuture.completedFuture(new CreateStoreResponse.Success());
  }

  public CompletableFuture<DeleteStoreResponse> deleteStore(String storeName) {
    return CompletableFuture.completedFuture(new DeleteStoreResponse.Success());
  }

  public CompletableFuture<ListStoresResponse> listStores() {

    return CompletableFuture.completedFuture(new ListStoresResponse.Success());
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

  @Override
  public void close() {
    /*scsControlClient.close();
    scsDataClient.close();*/
  }
}
