package momento.sdk;

import java.util.concurrent.CompletableFuture;
import momento.sdk.responses.storage.control.CreateStoreResponse;
import momento.sdk.responses.storage.control.DeleteStoreResponse;
import momento.sdk.responses.storage.control.ListStoresResponse;
import momento.sdk.responses.storage.data.DeleteResponse;
import momento.sdk.responses.storage.data.GetResponse;
import momento.sdk.responses.storage.data.PutResponse;

/**
 * Client for interacting with the preview storage service.
 *
 * <p>Note: This is a preview service and the API is subject to change.
 */
public interface IPreviewStorageClient {
  /** Control operations */

  /**
   * Create a new store.
   *
   * @param storeName Name of the store to create.
   * @return A future that will complete with the response.
   */
  CompletableFuture<CreateStoreResponse> createStore(String storeName);

  /**
   * Delete a store.
   *
   * @param storeName Name of the store to delete.
   * @return A future that will complete with the response.
   */
  CompletableFuture<DeleteStoreResponse> deleteStore(String storeName);

  /**
   * List all stores.
   *
   * @return A future that will complete with the response.
   */
  CompletableFuture<ListStoresResponse> listStores();

  /** Data operations */

  /**
   * Get a value from a store.
   *
   * @param storeName Name of the store to get from.
   * @param key Key of the value to get.
   * @return A future that will complete with the response. If the key does not exist, the response
   *     will be an error.
   */
  CompletableFuture<GetResponse> get(String storeName, String key);

  /**
   * Put a value into a store.
   *
   * @param storeName Name of the store to put into.
   * @param key Key of the value to put.
   * @param value Value to put.
   * @return A future that will complete with the response.
   */
  CompletableFuture<PutResponse> put(String storeName, String key, byte[] value);

  /**
   * Put a value into a store.
   *
   * @param storeName Name of the store to put into.
   * @param key Key of the value to put.
   * @param value Value to put.
   * @return A future that will complete with the response.
   */
  CompletableFuture<PutResponse> put(String storeName, String key, String value);

  /**
   * Put a value into a store.
   *
   * @param storeName Name of the store to put into.
   * @param key Key of the value to put.
   * @param value Value to put.
   * @return A future that will complete with the response.
   */
  CompletableFuture<PutResponse> put(String storeName, String key, long value);

  /**
   * Put a value into a store.
   *
   * @param storeName Name of the store to put into.
   * @param key Key of the value to put.
   * @param value Value to put.
   * @return A future that will complete with the response.
   */
  CompletableFuture<PutResponse> put(String storeName, String key, double value);

  /**
   * Delete a value from a store.
   *
   * @param storeName Name of the store to delete from.
   * @param key Key of the value to delete.
   * @return A future that will complete with the response. If the key does not exist, the response
   *     will be a success.
   */
  CompletableFuture<DeleteResponse> delete(String storeName, String key);
}
