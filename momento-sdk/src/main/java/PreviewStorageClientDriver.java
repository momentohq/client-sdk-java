import momento.sdk.IPreviewStorageClient;
import momento.sdk.PreviewStorageClient;
import momento.sdk.exceptions.ClientSdkException;
import momento.sdk.responses.storage.control.CreatePersistentStoreResponse;
import momento.sdk.responses.storage.control.ListPersistentStoresResponse;
import momento.sdk.responses.storage.data.GetResponse;

public class PreviewStorageClientDriver {
  public static void main(String[] args) {
    // Instantiating the client will be largely the same as compared to cache/topics.
    IPreviewStorageClient client = new PreviewStorageClient();

    // Create a store
    CreatePersistentStoreResponse createResponse = client.createStore("myStore").join();
    if (createResponse instanceof CreatePersistentStoreResponse.Success) {
      System.out.println("Store created successfully");
    } else {
      // Would inspect for Already exists, etc.
      System.out.println("Error creating store");
    }

    // List all stores
    final ListPersistentStoresResponse listResponse = client.listStores().join();
    if (listResponse instanceof ListPersistentStoresResponse.Success) {
      ListPersistentStoresResponse.Success success =
          (ListPersistentStoresResponse.Success) listResponse;
      System.out.println("Stores:");
      success.getStores().forEach(store -> System.out.println("- Store: " + store.getName()));
    } else if (listResponse instanceof ListPersistentStoresResponse.Error) {
      System.out.println(
          "Error listing stores: "
              + ((ListPersistentStoresResponse.Error) listResponse).getMessage());
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

    if (stringResponse instanceof GetResponse.Success) {
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
    } else if (stringResponse instanceof GetResponse.Error) {
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
