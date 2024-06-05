import momento.sdk.IPreviewStorageClient;
import momento.sdk.PreviewStorageClient;
import momento.sdk.auth.CredentialProvider;
import momento.sdk.config.StorageConfigurations;
import momento.sdk.exceptions.ClientSdkException;
import momento.sdk.responses.storage.control.CreateStoreResponse;
import momento.sdk.responses.storage.control.ListStoresResponse;
import momento.sdk.responses.storage.data.GetResponse;

public class PreviewStorageClientDriver {
  public static void main(String[] args) {
    // Instantiating the client will be largely the same as compared to cache/topics.
    IPreviewStorageClient client =
        new PreviewStorageClient(
            CredentialProvider.fromEnvVar("MOMENTO_API_KEY"),
            StorageConfigurations.Laptop.latest());

    // Create a store
    CreateStoreResponse createResponse = client.createStore("myStore").join();
    if (createResponse instanceof CreateStoreResponse.Success) {
      System.out.println("Store created successfully");
    } else {
      // Would inspect for Already exists, etc.
      System.out.println("Error creating store");
    }

    // List all stores
    final ListStoresResponse listResponse = client.listStores().join();
    if (listResponse instanceof ListStoresResponse.Success) {
      ListStoresResponse.Success success = (ListStoresResponse.Success) listResponse;
      System.out.println("Stores:");
      success.getStores().forEach(store -> System.out.println("- Store: " + store.getName()));
    } else if (listResponse instanceof ListStoresResponse.Error) {
      System.out.println(
          "Error listing stores: " + ((ListStoresResponse.Error) listResponse).getMessage());
    } else {
      System.out.println("Unknown response type");
    }

    // Set a value in the store
    client.set("myStore", "myKey", "myValue").join();

    // Get the value from the store
    final GetResponse stringResponse = client.get("myStore", "string").join();
    printGenericGetResponse(stringResponse);

    final GetResponse doubleResponse = client.get("myStore", "double").join();
    printGenericGetResponse(doubleResponse);

    if (doubleResponse instanceof GetResponse.Success) {
      GetResponse.Success success = (GetResponse.Success) doubleResponse;
      System.out.println("Value type for \"double\" was: " + success.getType());
      System.out.println("Value: " + success.getValueAsDouble());

      // Exception
      try {
        System.out.println("Value: " + success.getValueAsString());
      } catch (ClientSdkException e) {
        System.out.println(
            "Error trying to access the value of \"double\" as a string: " + e.getMessage());
      }
    } else if (doubleResponse instanceof GetResponse.Error) {
      GetResponse.Error error = (GetResponse.Error) doubleResponse;
      System.out.println("Error: " + error.getMessage());
    } else {
      System.out.println("Unknown response type");
    }
  }

  /** Demos how to handle a get response with an unknown type. */
  public static void printGenericGetResponse(GetResponse response) {
    if (response instanceof GetResponse.Success) {
      final GetResponse.Success success = (GetResponse.Success) response;
      // IntelliJ has an option to enforce enum switch completeness
      switch (success.getType()) {
        case STRING:
          System.out.println("Value: " + success.getValueAsString());
          break;
        case BYTE_ARRAY:
          System.out.println("Value: " + new String(success.getValueAsByteArray()));
          break;
        case LONG:
          System.out.println("Value: " + success.getValueAsLong());
          break;
        case DOUBLE:
          System.out.println("Value: " + success.getValueAsDouble());
          break;
      }
    } else if (response instanceof GetResponse.Error) {
      final GetResponse.Error error = (GetResponse.Error) response;
      System.out.println("Error: " + error.getMessage());
    }
  }
}