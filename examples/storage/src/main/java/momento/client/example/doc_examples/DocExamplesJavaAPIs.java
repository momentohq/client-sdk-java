package momento.client.example.doc_examples;

import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;
import momento.sdk.PreviewStorageClient;
import momento.sdk.auth.CredentialProvider;
import momento.sdk.config.StorageConfigurations;
import momento.sdk.exceptions.StoreAlreadyExistsException;
import momento.sdk.responses.storage.*;

public class DocExamplesJavaAPIs {

  @SuppressWarnings("EmptyTryBlock")
  public static void example_API_Storage_InstantiateClient() {
    try (PreviewStorageClient storageClient =
        new PreviewStorageClient(
            CredentialProvider.fromEnvVar("MOMENTO_API_KEY"),
            StorageConfigurations.Laptop.latest())) {
      // ...
    }
  }

  public static void example_API_Storage_CreateStore(PreviewStorageClient storageClient) {
    final CreateStoreResponse response = storageClient.createStore("test-store").join();
    if (response instanceof CreateStoreResponse.Success) {
      System.out.println("Store 'test-store' created");
    } else if (response instanceof CreateStoreResponse.Error error) {
      if (error.getCause() instanceof StoreAlreadyExistsException) {
        System.out.println("Store 'test-store' already exists");
      } else {
        throw new RuntimeException(
            "An error occurred while attempting to create store 'test-store': "
                + error.getErrorCode(),
            error);
      }
    }
  }

  public static void example_API_Storage_DeleteStore(PreviewStorageClient storageClient) {
    final DeleteStoreResponse response = storageClient.deleteStore("test-store").join();
    if (response instanceof DeleteStoreResponse.Success) {
      System.out.println("Store 'test-store' deleted");
    } else if (response instanceof DeleteStoreResponse.Error error) {
      throw new RuntimeException(
          "An error occurred while attempting to delete store 'test-store': "
              + error.getErrorCode(),
          error);
    }
  }

  public static void example_API_Storage_ListStores(PreviewStorageClient storageClient) {
    final ListStoresResponse response = storageClient.listStores().join();
    if (response instanceof ListStoresResponse.Success success) {
      final String stores =
          success.getStores().stream().map(StoreInfo::getName).collect(Collectors.joining("\n"));
      System.out.println("Stores:\n" + stores);
    } else if (response instanceof ListStoresResponse.Error error) {
      throw new RuntimeException(
          "An error occurred while attempting to list stores: " + error.getErrorCode(), error);
    }
  }

  public static void example_API_Storage_Put(PreviewStorageClient storageClient) {
    // this example illustrates how to store a String value
    final PutResponse response = storageClient.put("test-store", "test-key", "test-value").join();
    if (response instanceof PutResponse.Success) {
      System.out.println("Key 'test-key' stored successfully");
    } else if (response instanceof PutResponse.Error error) {
      throw new RuntimeException(
          "An error occurred while attempting to store key 'test-key' in store 'test-store': "
              + error.getErrorCode(),
          error);
    }

    // Momento Storage also supports storing values of type byte[], long, and double:
    byte[] bytesValue = "test-byte-array-value".getBytes(StandardCharsets.UTF_8);
    storageClient.put("test-store", "test-byte-array-key", bytesValue).join();
    storageClient.put("test-store", "test-integer-key", 42L).join();
    storageClient.put("test-store", "test-double-key", 42.0).join();
  }

  public static void example_API_Storage_Get(PreviewStorageClient storageClient) {
    final GetResponse response = storageClient.get("test-store", "test-key").join();

    // simplified style to access the value, if you're confident the value exists and you know the
    // type.
    // The optionals in this chain will throw exceptions when you call `.get()` if the item did not
    // exist in the store, or is another type besides a String
    final String value = response.valueWhenFound().get().getString().get();

    // Or, you can use pattern-matching for more production-safe code:
    if (response instanceof GetResponse.Found found) {
      // if you know the value is a String:
      String stringValue =
          found
              .value()
              .getString()
              .orElseThrow(() -> new RuntimeException("Value was not a String!"));
      // if you don't know the type of the value:
      switch (found.value().getType()) {
        case STRING -> System.out.println("String value: " + found.value().getString().get());
        case BYTE_ARRAY -> System.out.println(
            "Byte array value: " + found.value().getByteArray().get());
        case LONG -> System.out.println("Long value: " + found.value().getLong().get());
        case DOUBLE -> System.out.println("Double value: " + found.value().getDouble().get());
      }
    } else if (response instanceof GetResponse.NotFound) {
      System.out.println("Key 'test-key' was not found in store 'test-store'");
    } else if (response instanceof GetResponse.Error error) {
      throw new RuntimeException(
          "An error occurred while attempting to get key 'test-key' from store 'test-store': "
              + error.getErrorCode(),
          error);
    }
  }

  public static void example_API_Storage_Delete(PreviewStorageClient storageClient) {
    final DeleteResponse response = storageClient.delete("test-store", "test-key").join();
    if (response instanceof DeleteResponse.Success) {
      System.out.println("Key 'test-key' deleted successfully");
    } else if (response instanceof DeleteResponse.Error error) {
      throw new RuntimeException(
          "An error occurred while attempting to delete key 'test-key' from store 'test-store': "
              + error.getErrorCode(),
          error);
    }
  }

  public static void main(String[] args) {
    example_API_Storage_InstantiateClient();
    try (final PreviewStorageClient storageClient =
        new PreviewStorageClient(
            CredentialProvider.fromEnvVar("MOMENTO_API_KEY"),
            StorageConfigurations.Laptop.latest())) {

      example_API_Storage_CreateStore(storageClient);
      example_API_Storage_DeleteStore(storageClient);
      example_API_Storage_CreateStore(storageClient);
      example_API_Storage_ListStores(storageClient);

      example_API_Storage_Put(storageClient);
      example_API_Storage_Get(storageClient);
      example_API_Storage_Delete(storageClient);
    }
  }
}
