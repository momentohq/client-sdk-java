package momento.client.example;

import momento.sdk.PreviewStorageClient;
import momento.sdk.auth.CredentialProvider;
import momento.sdk.config.StorageConfigurations;
import momento.sdk.exceptions.StoreAlreadyExistsException;
import momento.sdk.responses.storage.CreateStoreResponse;
import momento.sdk.responses.storage.GetResponse;
import momento.sdk.responses.storage.ListStoresResponse;
import momento.sdk.responses.storage.StoreInfo;

import java.nio.charset.StandardCharsets;

public class BasicExample {

  private static final String API_KEY_ENV_VAR = "MOMENTO_API_KEY";

  private static final String STORE_NAME = "store";
  private static final String KEY = "key";
  private static final String VALUE = "value";

  public static void main(String[] args) {
    printStartBanner();

    final CredentialProvider credentialProvider = CredentialProvider.fromEnvVar(API_KEY_ENV_VAR);

    try (final var client =
        new PreviewStorageClient(credentialProvider, StorageConfigurations.Laptop.latest())) {

      createStore(client, STORE_NAME);

      listStores(client);

      System.out.printf("Putting key '%s', value '%s'%n", KEY, VALUE);
      // store a string value
      client.put(STORE_NAME, KEY, VALUE).join();

      // you can also store values of type byte[], long, or double:
      byte[] bytesValue = "value".getBytes(StandardCharsets.UTF_8);
      client.put(STORE_NAME, "bytes-key", bytesValue).join();
      client.put(STORE_NAME, "long-key", 42L).join();
      client.put(STORE_NAME, "double-key", 3.14).join();

      System.out.printf("Getting value for key '%s'%n", KEY);

      final GetResponse getResponse = client.get(STORE_NAME, KEY).join();

      // simplified style, if you are confident the value exists and is a String:
      final String value = getResponse.valueWhenFound().get().getString().get();

      // pattern-matching style, more suitable for production code:
      if (getResponse instanceof GetResponse.Found hit) {
        // if you're confident it's a String:
        System.out.printf("Found value for key '%s': '%s'%n", KEY, hit.value().getString().get());

        // you're not sure of the type:
        switch(hit.value().getType()) {
            case BYTE_ARRAY -> { System.out.println("Got a byte array: " + hit.value().getByteArray().get()); }
            case STRING -> { System.out.println("Got a string: " + hit.value().getString().get()); }
            case LONG -> { System.out.println("Got a long: " + hit.value().getLong().get()); }
            case DOUBLE -> { System.out.println("Got a double: " + hit.value().getDouble().get());}
        }
      } else if (getResponse instanceof GetResponse.NotFound) {
        System.out.println("Found no value for key " + KEY);
      } else if (getResponse instanceof GetResponse.Error error) {
        System.out.printf(
            "Unable to look up value for key '%s' with error %s\n", KEY, error.getErrorCode());
        System.out.println(error.getMessage());
      }
    }
    printEndBanner();
  }

  private static void createStore(PreviewStorageClient storageClient, String storeName) {
    final CreateStoreResponse createResponse = storageClient.createStore(storeName).join();
    if (createResponse instanceof CreateStoreResponse.Error error) {
      if (error.getCause() instanceof StoreAlreadyExistsException) {
        System.out.println("Store with name '" + storeName + "' already exists.");
      } else {
        System.out.println("Unable to create store with error " + error.getErrorCode());
        System.out.println(error.getMessage());
      }
    }
  }

  private static void listStores(PreviewStorageClient storageClient) {
    System.out.println("Listing caches:");
    final ListStoresResponse listResponse = storageClient.listStores().join();
    if (listResponse instanceof ListStoresResponse.Success success) {
      for (StoreInfo storeInfo : success.getStores()) {
        System.out.println(storeInfo.getName());
      }
    } else if (listResponse instanceof ListStoresResponse.Error error) {
      System.out.println("Unable to list caches with error " + error.getErrorCode());
      System.out.println(error.getMessage());
    }
  }

  private static void printStartBanner() {
    System.out.println("******************************************************************");
    System.out.println("Basic Example Start");
    System.out.println("******************************************************************");
  }

  private static void printEndBanner() {
    System.out.println("******************************************************************");
    System.out.println("Basic Example End");
    System.out.println("******************************************************************");
  }
}
