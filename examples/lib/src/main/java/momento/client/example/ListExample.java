package momento.client.example;

import java.util.List;
import momento.sdk.CacheClient;
import momento.sdk.exceptions.AlreadyExistsException;
import momento.sdk.messages.CacheListConcatenateFrontResponse;
import momento.sdk.messages.CacheListFetchResponse;
import momento.sdk.messages.CacheListLengthResponse;
import momento.sdk.messages.CacheListPushFrontResponse;
import momento.sdk.messages.CacheListRemoveValueResponse;
import momento.sdk.messages.CreateCacheResponse;

public class ListExample extends AbstractExample {

  private static final String CACHE_NAME = "list-example-cache";
  private static final String LIST_NAME = "example-list";

  public static void main(String[] args) {
    printStartBanner("List");

    try (final CacheClient client = buildCacheClient()) {
      // create a cache
      final CreateCacheResponse createCacheResponse = client.createCache(CACHE_NAME);
      if (createCacheResponse instanceof CreateCacheResponse.Error error) {
        if (error.getCause() instanceof AlreadyExistsException) {
          System.out.println("Cache with name '" + CACHE_NAME + "' already exists.");
        } else {
          System.out.println("Unable to create cache with error: " + error.getMessage());
        }
      }

      // add elements to a list
      System.out.println("Adding elements to " + LIST_NAME);

      final CacheListPushFrontResponse pushResponse =
          client.listPushFront(CACHE_NAME, LIST_NAME, "element4", 100).join();
      if (pushResponse instanceof CacheListPushFrontResponse.Error error) {
        System.out.println("List push failed with error: " + error.getMessage());
      }

      final List<String> elements = List.of("element1", "element2", "element2", "element3");
      final CacheListConcatenateFrontResponse concatenateResponse =
          client.listConcatenateFront(CACHE_NAME, LIST_NAME, elements, 100).join();
      if (concatenateResponse instanceof CacheListConcatenateFrontResponse.Error error) {
        System.out.println("List concatenate failed with error: " + error.getMessage());
      }

      // fetch the list
      System.out.println("Fetching " + LIST_NAME);

      final CacheListFetchResponse fetchResponse =
          client.listFetch(CACHE_NAME, LIST_NAME, null, null).join();
      if (fetchResponse instanceof CacheListFetchResponse.Hit hit) {
        final List<String> fetchedElements = hit.valueList();
        System.out.println(LIST_NAME + " has length " + fetchedElements.size() + " with elements:");
        System.out.println(String.join(", ", fetchedElements));
      } else if (fetchResponse instanceof CacheListFetchResponse.Miss) {
        System.out.println("Did not find list with name " + LIST_NAME);
      } else if (fetchResponse instanceof CacheListFetchResponse.Error error) {
        System.out.println("List fetch failed with error: " + error.getMessage());
      }

      // Remove an element
      System.out.println("Removing an element that occurs multiple times from " + LIST_NAME);

      final CacheListRemoveValueResponse removeResponse =
          client.listRemoveValue(CACHE_NAME, LIST_NAME, "element2").join();
      if (removeResponse instanceof CacheListRemoveValueResponse.Error error) {
        System.out.println("List remove failed with error: " + error.getMessage());
      }

      // check length
      System.out.println("Checking length of " + LIST_NAME);

      final CacheListLengthResponse lengthResponse =
          client.listLength(CACHE_NAME, LIST_NAME).join();
      if (lengthResponse instanceof CacheListLengthResponse.Hit hit) {
        System.out.println(LIST_NAME + " length: " + hit.getListLength());
      } else if (lengthResponse instanceof CacheListLengthResponse.Miss) {
        System.out.println("Did not find list with name " + LIST_NAME);
      } else if (lengthResponse instanceof CacheListLengthResponse.Error error) {
        System.out.println("List length failed with error: " + error.getMessage());
      }
    }

    printEndBanner("List");
  }
}
